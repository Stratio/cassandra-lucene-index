/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene

import java.lang.reflect.{Field, Modifier}
import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.IndexQueryHandler._
import com.stratio.cassandra.lucene.partitioning.Partitioner
import com.stratio.cassandra.lucene.util.{Logging, TimeCounter}
import org.apache.cassandra.cql3._
import org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull
import org.apache.cassandra.cql3.statements.{BatchStatement, IndexTarget, ParsedStatement, SelectStatement}
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter.{CustomExpression, Expression}
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.service.{ClientState, LuceneStorageProxy, QueryState}
import org.apache.cassandra.transport.messages.ResultMessage
import org.apache.cassandra.transport.messages.ResultMessage.{Prepared, Rows}
import org.apache.cassandra.utils.{FBUtilities, MD5Digest}

import scala.collection.JavaConverters._
import scala.collection.mutable


/** [[QueryHandler]] to be used with Lucene searches.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexQueryHandler extends QueryHandler with Logging {

  type Payload = java.util.Map[String, ByteBuffer]

  /** @inheritdoc */
  override def prepare(query: String, state: QueryState, payload: Payload): Prepared = {
    QueryProcessor.instance.prepare(query, state)
  }

  /** @inheritdoc */
  override def getPrepared(id: MD5Digest): ParsedStatement.Prepared = {
    QueryProcessor.instance.getPrepared(id)
  }

  /** @inheritdoc */
  override def getPreparedForThrift(id: Integer): ParsedStatement.Prepared = {
    QueryProcessor.instance.getPreparedForThrift(id)
  }

  /** @inheritdoc */
  override def processBatch(
      statement: BatchStatement,
      state: QueryState,
      options: BatchQueryOptions,
      payload: Payload): ResultMessage = {
    QueryProcessor.instance.processBatch(statement, state, options, payload)
  }

  /** @inheritdoc */
  override def processPrepared(
      statement: CQLStatement,
      state: QueryState,
      options: QueryOptions,
      payload: Payload): ResultMessage = {
    QueryProcessor.metrics.preparedStatementsExecuted.inc()
    processStatement(statement, state, options)
  }

  /** @inheritdoc */
  override def process(
      query: String,
      state: QueryState,
      options: QueryOptions,
      payload: Payload): ResultMessage = {
    val p = QueryProcessor.getStatement(query, state.getClientState)
    options.prepare(p.boundNames)
    val prepared = p.statement
    if (prepared.getBoundTerms != options.getValues.size) {
      throw new InvalidRequestException("Invalid amount of bind variables")
    }
    if (!state.getClientState.isInternal) {
      QueryProcessor.metrics.regularStatementsExecuted.inc()
    }
    processStatement(prepared, state, options)
  }

  def processStatement(
      statement: CQLStatement,
      state: QueryState,
      options: QueryOptions): ResultMessage = {

    logger.trace(s"Process $statement @CL.${options.getConsistency}")
    val clientState = state.getClientState
    statement.checkAccess(clientState)
    statement.validate(clientState)

    // Intercept Lucene index searches
    statement match {
      case select: SelectStatement =>
        val expressions = luceneExpressions(select, options)
        if (expressions.nonEmpty) {
          val time = TimeCounter.start
          try {
            return executeLuceneQuery(select, state, options, expressions)
          } catch {
            case e: ReflectiveOperationException => throw new IndexException(e)
          } finally {
            logger.debug(s"Lucene search total time: $time\n")
          }
        }
      case _ =>
    }
    execute(statement, state, options)
  }

  def luceneExpressions(
      select: SelectStatement,
      options: QueryOptions): Map[Expression, Index] = {
    val map = mutable.LinkedHashMap.empty[Expression, Index]
    val expressions = select.getRowFilter(options).getExpressions
    val cfs = Keyspace.open(select.keyspace).getColumnFamilyStore(select.columnFamily)
    val indexes = cfs.indexManager.listIndexes.asScala.collect { case index: Index => index }
    expressions.forEach {
      case expression: CustomExpression =>
        val clazz = expression.getTargetIndex.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME)
        if (clazz == classOf[Index].getCanonicalName) {
          val index = cfs.indexManager.getIndex(expression.getTargetIndex).asInstanceOf[Index]
          map += expression -> index
        }
      case expr: Expression =>
        indexes.filter(_.supportsExpression(expr.column, expr.operator)).foreach(map.put(expr, _))
    }
    map.toMap
  }

  def execute(statement: CQLStatement, state: QueryState, options: QueryOptions): ResultMessage = {
    val result = statement.execute(state, options)
    if (result == null) new ResultMessage.Void else result
  }

  def executeLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions,
      expressions: Map[Expression, Index]): ResultMessage = {

    if (expressions.size > 1) {
      throw new InvalidRequestException(
        "Lucene index only supports one search expression per query.")
    }

    // Validate expression
    val (expression, index) = expressions.head
    val search = index.validate(expression)

    // Get partitioner
    val partitioner = index.service.partitioner

    // Get paging info
    val limit = select.getLimit(options)
    val page = getPageSize.invoke(select, options).asInstanceOf[Int]

    // Take control of paging if there is paging and the query requires post processing
    if (search.requiresPostProcessing && page > 0 && page < limit) {
      executeSortedLuceneQuery(select, state, options, partitioner)
    } else {
      execute(select, state, options)
    }
  }

  def executeSortedLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions,
      partitioner: Partitioner): Rows = {

    // Check consistency level
    val consistency = options.getConsistency
    checkNotNull(consistency, "Invalid empty consistency level")
    consistency.validateForRead(select.keyspace)

    val now = FBUtilities.nowInSeconds
    val limit = select.getLimit(options)
    val page = options.getPageSize

    // Read paging state and write it to query
    val pagingState = IndexPagingState.build(options.getPagingState, limit)
    val query = select.getQuery(options, now, Math.min(page, pagingState.remaining))
    pagingState.rewrite(query)

    // Read data
    val data = query match {
      case group: Group if group.commands.size > 1 => LuceneStorageProxy.read(group, consistency)
      case _ => query.execute(consistency, state.getClientState)
    }

    // Process data updating paging state
    try {
      val processedData = pagingState.update(query, data, consistency, partitioner)
      val rows = processResults.invoke(
        select,
        processedData,
        options,
        now.asInstanceOf[AnyRef],
        page.asInstanceOf[AnyRef]).asInstanceOf[Rows]
      rows.result.metadata.setHasMorePages(pagingState.toPagingState)
      rows
    } finally {
      if (data != null) data.close()
    }
  }
}

/** Companion object for [[IndexQueryHandler]]. */
object IndexQueryHandler {

  val getPageSize = classOf[SelectStatement].getDeclaredMethod("getPageSize", classOf[QueryOptions])
  getPageSize.setAccessible(true)

  val processResults = classOf[SelectStatement].getDeclaredMethod(
    "processResults", classOf[PartitionIterator], classOf[QueryOptions], classOf[Int], classOf[Int])
  processResults.setAccessible(true)

  /** Sets this query handler as the Cassandra CQL query handler, replacing the previous one. */
  def activate(): Unit = {
    this.synchronized {
      if (!ClientState.getCQLQueryHandler.isInstanceOf[IndexQueryHandler]) {
        try {
          val field = classOf[ClientState].getDeclaredField("cqlQueryHandler")
          field.setAccessible(true)
          val modifiersField = classOf[Field].getDeclaredField("modifiers")
          modifiersField.setAccessible(true)
          modifiersField.setInt(field, field.getModifiers & ~Modifier.FINAL)
          field.set(null, new IndexQueryHandler)
        } catch {
          case e: Exception => throw new IndexException("Unable to set Lucene CQL query handler", e)
        }
      }
    }
  }

}
