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

import java.util.concurrent.Callable
import java.util.function.BiFunction
import java.util.{Collections, Optional}
import java.{util => java}

import com.stratio.cassandra.lucene.search.Search
import com.stratio.cassandra.lucene.util.Logging
import org.apache.cassandra.config.{CFMetaData, ColumnDefinition}
import org.apache.cassandra.cql3.Operator
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter
import org.apache.cassandra.db.marshal.{AbstractType, UTF8Type}
import org.apache.cassandra.db.partitions._
import org.apache.cassandra.exceptions.{ConfigurationException, InvalidRequestException}
import org.apache.cassandra.index.Index.{Indexer, Searcher}
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.index.{IndexRegistry, Index => CassandraIndex}
import org.apache.cassandra.schema.IndexMetadata
import org.apache.cassandra.utils.concurrent.OpOrder


/** [[CassandraIndex]] that uses Apache Lucene as backend. It allows, among
  * others, multi-column and full-text search.
  *
  * @param table         the indexed table
  * @param indexMetadata the index's metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class Index(table: ColumnFamilyStore, indexMetadata: IndexMetadata)
  extends CassandraIndex with Logging {

  // Set Lucene query handler as CQL query handler
  IndexQueryHandler.activate()

  logger.debug(s"Building Lucene index ${table.metadata} $indexMetadata")

  val service = try IndexService.build(table, indexMetadata) catch {
    case e: Exception => throw new IndexException(e)
  }

  val name = service.qualifiedName

  /** Return a task to perform any initialization work when a new index instance is created. This
    * may involve costly operations such as (re)building the index, and is performed asynchronously
    * by SecondaryIndexManager.
    *
    * @return a task to perform any necessary initialization work
    */
  override def getInitializationTask: Callable[_] = {
    if (table.isEmpty || SystemKeyspace.isIndexBuilt(table.keyspace.getName, indexMetadata.name)) {
      logger.info(s"Index $name doesn't need (re)building")
      null
    } else {
      logger.info(s"Index $name needs (re)building")
      getBuildIndexTask
    }
  }

  private[this] def getBuildIndexTask: Callable[_] = () => {
    table.forceBlockingFlush()
    service.truncate()
    table.indexManager.buildIndexBlocking(Index.this)
  }

  /** Returns the IndexMetadata which configures and defines the index instance. This should be the
    * same object passed as the argument to setIndexMetadata.
    *
    * @return the index's metadata
    */
  override def getIndexMetadata: IndexMetadata = indexMetadata

  /** Return a task to reload the internal metadata of an index. Called when the base table metadata
    * is modified or when the configuration of the Index is updated Implementations should return a
    * task which performs any necessary work to be done due to updating the configuration(s) such as
    * (re)building etc. This task is performed asynchronously by SecondaryIndexManager.
    *
    * @return task to be executed by the index manager during a reload
    */
  override def getMetadataReloadTask(indexMetadata: IndexMetadata): Callable[_] = () => {
    // TODO: Check return getBuildIndexTask if index metadata is different
    logger.debug(s"Reloading Lucene index $name metadata: $indexMetadata")
  }

  /** An index must be registered in order to be able to either subscribe to update events on the
    * base table and/or to provide IndexSearcher functionality for reads. The double dispatch
    * involved here, where the Index actually performs its own registration by calling back to the
    * supplied IndexRegistry's own registerIndex method, is to make the decision as to whether or
    * not to register an index belong to the implementation, not the manager.
    *
    * @param registry the index registry to register the instance with
    */
  override def register(registry: IndexRegistry) {
    registry.registerIndex(this)
  }

  /** If the index implementation uses a local table to store its index data this method should
    * return a handle to it. If not, an empty Optional should be returned. Typically, this is useful
    * for the built-in Index implementations.
    *
    * @return the Index's backing storage table
    */
  override def getBackingTable: Optional[ColumnFamilyStore] = Optional.empty()

  /** Return a task which performs a blocking flush of the index's data to persistent storage.
    *
    * @return task to be executed by the index manager to perform the flush
    */
  override def getBlockingFlushTask: Callable[_] = () => {
    logger.info(s"Flushing Lucene index $name")
    service.commit()
  }

  /** Return a task which invalidates the index, indicating it should no longer be considered
    * usable. This should include an clean up and releasing of resources required when dropping an
    * index.
    *
    * @return task to be executed by the index manager to invalidate the index
    */
  override def getInvalidateTask: Callable[_] = () => {
    logger.info(s"Invalidating Lucene index $name")
    service.delete()
  }

  /** Return a task to truncate the index with the specified truncation timestamp. Called when the
    * base table is truncated.
    *
    * @param truncatedAt timestamp of the truncation operation. This will be the same timestamp used
    *                    in the truncation of the base table.
    * @return task to be executed by the index manager when the base table is truncated.
    */
  override def getTruncateTask(truncatedAt: Long): Callable[_] = () => {
    logger.info(s"Truncating Lucene index $name")
    service.truncate()
  }

  /** Return true if this index can be built or rebuilt when the index manager determines it is
    * necessary. Returning false enables the index implementation (or some other component) to
    * control if and when SSTable data is incorporated into the index.
    *
    * This is called by SecondaryIndexManager in buildIndexBlocking, buildAllIndexesBlocking and
    * rebuildIndexesBlocking where a return value of false causes the index to be excluded from the
    * set of those which will process the SSTable data.
    *
    * @return if the index should be included in the set which processes SSTable data
    */
  override def shouldBuildBlocking: Boolean = {
    true
  }

  /** Called to determine whether this index targets a specific column. Used during schema
    * operations such as when dropping or renaming a column, to check if the index will be affected
    * by the change. Typically, if an index answers that it does depend upon a column, then schema
    * operations on that column are not permitted until the index is dropped or altered.
    *
    * @param column the column definition to check
    * @return true if the index depends on the supplied column being present; false if the column
    *         may be safely dropped or modified without adversely affecting the index
    */
  override def dependsOn(column: ColumnDefinition): Boolean = {
    // TODO: Could return true only for key and/or mapped columns?
    logger.trace(s"Asking if the index depends on column $column")
    service.dependsOn(column)
  }

  /** Called to determine whether this index can provide a searcher to execute a query on the
    * supplied column using the specified operator. This forms part of the query validation done
    * before a CQL select statement is executed.
    *
    * @param column   the target column of a search query predicate
    * @param operator the operator of a search query predicate
    * @return true if this index is capable of supporting such expressions, false otherwise
    */
  override def supportsExpression(column: ColumnDefinition, operator: Operator): Boolean = {
    logger.trace(s"Asking if the index supports the expression $column $operator")
    service.expressionMapper.supports(column, operator)
  }

  /** If the index supports custom search expressions using the {{{SELECT * FROM table WHERE
    * expr(index_name, expression)}}} syntax, this method should return the expected type of the
    * expression argument. For example, if the index supports custom expressions as Strings, calls
    * to this method should return `UTF8Type.instance`. If the index implementation does not support
    * custom expressions, then it should return null.
    *
    * @return the type of custom expressions supported by this index, or null if custom expressions
    *         are not supported.
    */
  override def customExpressionValueType: AbstractType[_] = {
    logger.trace("Requesting the custom expressions value type")
    UTF8Type.instance
  }

  /** Transform an initial RowFilter into the filter that will still need to applied to a set of
    * Rows after the index has performed it's initial scan. Used in ReadCommand#executeLocal to
    * reduce the amount of filtering performed on the results of the index query.
    *
    * @param filter the initial filter belonging to a ReadCommand
    * @return the (hopefully) reduced filter that would still need to be applied after the index was
    *         used to narrow the initial result set
    */
  override def getPostIndexQueryFilter(filter: RowFilter): RowFilter = {
    logger.trace(s"Getting the post index query filter for $filter")
    service.expressionMapper.postIndexQueryFilter(filter)
  }

  /** Return an estimate of the number of results this index is expected to return for any given
    * query that it can be used to answer. Used in conjunction with indexes() and
    * supportsExpression() to determine the most selective index for a given ReadCommand.
    * Additionally, this is also used by StorageProxy.estimateResultsPerRange to calculate the
    * initial concurrency factor for range requests
    *
    * @return the estimated average number of results aIndexSearcher may return for any given query
    */
  override def getEstimatedResultRows: Long = {
    logger.trace("Getting the estimated result rows")
    1
  }

  /** Called at write time to ensure that values present in the update are valid according to the
    * rules of all registered indexes which will process it. The partition key as well as the
    * clustering and cell values for each row in the update may be checked by index implementations
    *
    * @param update PartitionUpdate containing the values to be validated by registered indexes.
    * @throws InvalidRequestException If the update doesn't pass through the validation.
    */
  override def validate(update: PartitionUpdate) {
    logger.trace(s"Validating $update")
    try {
      service.validate(update)
    } catch {
      case e: Exception =>
        logger.debug(s"Invalid partition update: $update", e)
        throw new InvalidRequestException(e.getMessage)
    }
  }

  /** Creates an new indexer object for updates to a given partition.
    *
    * @param key             key of the partition being modified
    * @param columns         the regular and static columns the created indexer will have to deal
    *                        with. This can be empty as an update might only contain partition,
    *                        range and row deletions, but the indexer is guaranteed to not get any
    *                        cells for a column that is not part of columns.
    * @param nowInSec        current time of the update operation
    * @param opGroup         operation group spanning the update operation
    * @param transactionType indicates what kind of update is being performed on the base data i.e.
    *                        a write time insert/update/delete or the result of compaction
    * @return the newly created indexer or `null` if the index is not interested by the update (this
    *         could be because the index doesn't care about that particular partition, doesn't care
    *         about that type of transaction, ...).
    */
  override def indexerFor(
      key: DecoratedKey,
      columns: PartitionColumns,
      nowInSec: Int,
      opGroup: OpOrder.Group,
      transactionType: IndexTransaction.Type): Indexer = {
    service.writer(key, nowInSec, opGroup, transactionType)
  }

  /** Return a function which performs post processing on the results of a partition range read
    * command. In future, this may be used as a generalized mechanism for transforming results on
    * the coordinator prior to returning them to the caller.
    *
    * This is used on the coordinator during execution of a range command to perform post processing
    * of merged results obtained from the necessary replicas. This is the only way in which results
    * are transformed in this way but this may change over time as usage is generalized. See
    * CASSANDRA-8717 for further discussion.
    *
    * The function takes a PartitionIterator of the results from the replicas which has already been
    * collated and reconciled, along with the command being executed. It returns another
    * PartitionIterator containing the results of the transformation (which may be the same as the
    * input if the transformation is a no-op).
    */
  override def postProcessorFor(command: ReadCommand)
  : BiFunction[PartitionIterator, ReadCommand, PartitionIterator] = {
    new ReadCommandPostProcessor(service)
  }

  def postProcessorFor(group: Group): BiFunction[PartitionIterator, Group, PartitionIterator] = {
    new GroupPostProcessor(service)
  }

  /** Factory method for query time search helper. Custom index implementations should perform any
    * validation of query expressions here and throw a meaningful InvalidRequestException when any
    * expression is invalid.
    *
    * @param command the read command being executed
    * @return an IndexSearcher with which to perform the supplied command
    * @throws InvalidRequestException if the command's expressions are invalid according to the
    *                                 specific syntax supported by the index implementation.
    */
  override def searcherFor(command: ReadCommand): Searcher = {
    logger.trace(s"Getting searcher for $command")
    try {
      controller => service.search(command, controller)
    } catch {
      case e: Exception =>
        logger.error(s"Error getting searcher for command: $command", e)
        throw new InvalidRequestException(e.getMessage)
    }
  }

  /** Validates the specified custom expression.
    *
    * @param expression the expression to be validated
    * @return the valid search represented by `expression`
    * @throws InvalidRequestException if the expression is not valid
    */
  def validate(expression: RowFilter.Expression): Search = {
    try {
      service.validate(expression)
    } catch {
      case e: Exception =>
        logger.debug(s"Invalid index expression: $expression", e)
        throw new InvalidRequestException(e.getMessage)
    }
  }

}

/** Companion object for [[Index]]. */
object Index extends Logging {

  /** Validates the specified index options.
    *
    * @param options  the options to be validated
    * @param metadata the metadata of the table to be indexed
    * @return the validated options
    * @throws ConfigurationException if the options are not valid
    */
  def validateOptions(
      options: java.Map[String, String],
      metadata: CFMetaData): java.Map[String, String] = {
    logger.debug("Validating Lucene index options")
    try {
      IndexOptions.validate(options, metadata)
    } catch {
      case e: IndexException =>
        logger.error(s"Invalid index options: $options", e)
        throw new ConfigurationException(e.getMessage)
    }
    logger.debug("Lucene index options are valid")
    Collections.emptyMap[String, String]
  }

}