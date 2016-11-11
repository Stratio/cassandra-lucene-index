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

import java.nio.ByteBuffer
import java.{util => java}

import com.stratio.cassandra.lucene.IndexQueryHandler._
import com.stratio.cassandra.lucene.util.TimeCounter
import org.apache.cassandra.cql3._
import org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull
import org.apache.cassandra.cql3.statements.{BatchStatement, IndexTarget, ParsedStatement, SelectStatement}
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter.{CustomExpression, Expression}
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.service.{LuceneStorageProxy, QueryState}
import org.apache.cassandra.transport.messages.ResultMessage
import org.apache.cassandra.transport.messages.ResultMessage.{Prepared, Rows}
import org.apache.cassandra.utils.{FBUtilities, MD5Digest}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


/** [[QueryHandler]] to be used with Lucene searches.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexQueryHandler extends QueryHandler {

  type Payload = java.Map[String, ByteBuffer]

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
        if (!expressions.isEmpty) {
          val time = TimeCounter.create.start
          try {
            return executeLuceneQuery(select, state, options, expressions)
          } catch {
            case e: ReflectiveOperationException => throw new IndexException(e)
          } finally {
            logger.debug(s"Lucene search total time: ${time.stop}\n")
          }
        }
      case _ =>
    }
    execute(statement, state, options)
  }

  def luceneExpressions(
      select: SelectStatement,
      options: QueryOptions): java.Map[Expression, Index] = {
    val map = new java.LinkedHashMap[Expression, Index]
    val expressions = select.getRowFilter(options).getExpressions
    val cfs = Keyspace.open(select.keyspace).getColumnFamilyStore(select.columnFamily)
    val indexes = cfs.indexManager.listIndexes.collect { case index: Index => index }
    expressions.foreach {
      case expression: CustomExpression =>
        val clazz = expression.getTargetIndex.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME)
        if (clazz == classOf[Index].getCanonicalName) {
          val index = cfs.indexManager.getIndex(expression.getTargetIndex).asInstanceOf[Index]
          map.put(expression, index)
        }
      case expr =>
        indexes.filter(_.supportsExpression(expr.column, expr.operator)).foreach(map.put(expr, _))
    }
    map
  }

  def execute(statement: CQLStatement, state: QueryState, options: QueryOptions): ResultMessage = {
    val result = statement.execute(state, options)
    if (result == null) new ResultMessage.Void else result
  }

  @throws[ReflectiveOperationException]
  def executeLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions,
      expressions: java.Map[Expression, Index]): ResultMessage = {

    if (expressions.size > 1) {
      throw new InvalidRequestException(
        "Lucene index only supports one search expression per query.")
    }

    // Validate expression
    val expression = expressions.keys.head
    val index = expressions.get(expression)
    val search = index.validate(expression)

    // Get paging info
    val limit = select.getLimit(options)
    val page = getPageSize.invoke(select, options).asInstanceOf[Int]

    // Take control of paging if there is paging and the query requires post processing
    if (search.requiresPostProcessing && page > 0 && page < limit) {
      executeSortedLuceneQuery(select, state, options)
    } else {
      execute(select, state, options)
    }
  }

  @throws[ReflectiveOperationException]
  def executeSortedLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions): Rows = {

    // Check consistency level
    val consistency = options.getConsistency
    checkNotNull(consistency, "Invalid empty consistency level")
    consistency.validateForRead(select.keyspace)

    val now = FBUtilities.nowInSeconds
    val limit = select.getLimit(options)
    val userPerPartitionLimit = select.getPerPartitionLimit(options)
    val page = options.getPageSize

    // Read paging state and write it to query
    val pagingState = IndexPagingState.build(options.getPagingState, limit)
    val remaining = Math.min(page, pagingState.remaining)
    val query = select.getQuery(options, now, remaining, userPerPartitionLimit)
    pagingState.rewrite(query)

    // Read data
    val data = query match {
      case group: Group if group.commands.size > 1 => LuceneStorageProxy.read(group, consistency)
      case _ => query.execute(consistency, state.getClientState)
    }

    // Process data updating paging state
    try {
      val processedData = pagingState.update(query, data, consistency)
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

object IndexQueryHandler {

  val logger = LoggerFactory.getLogger(classOf[IndexQueryHandler])

  val getPageSize = classOf[SelectStatement].getDeclaredMethod("getPageSize", classOf[QueryOptions])
  getPageSize.setAccessible(true)

  val processResults = classOf[SelectStatement].getDeclaredMethod(
    "processResults", classOf[PartitionIterator], classOf[QueryOptions], classOf[Int], classOf[Int])
  processResults.setAccessible(true)

}
