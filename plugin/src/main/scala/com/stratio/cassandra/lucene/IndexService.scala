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

import java.lang.management.ManagementFactory
import java.{util => java}
import javax.management.{JMException, ObjectName}

import com.stratio.cassandra.lucene.IndexService._
import com.stratio.cassandra.lucene.column.Columns
import com.stratio.cassandra.lucene.index.{DocumentIterator, FSIndex, RAMIndex}
import com.stratio.cassandra.lucene.key.{PartitionMapper, TokenMapper}
import com.stratio.cassandra.lucene.search.{Search, SearchBuilder}
import com.stratio.cassandra.lucene.util._
import org.apache.cassandra.config.{CFMetaData, ColumnDefinition}
import org.apache.cassandra.cql3.Operator
import org.apache.cassandra.cql3.statements.IndexTarget
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter.CustomExpression
import org.apache.cassandra.db.filter._
import org.apache.cassandra.db.marshal.UTF8Type
import org.apache.cassandra.db.partitions._
import org.apache.cassandra.db.rows._
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.schema.IndexMetadata
import org.apache.cassandra.utils.concurrent.OpOrder
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.index.{IndexableField, Term}
import org.apache.lucene.search.{Query, ScoreDoc, Sort, SortField}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Lucene index service provider.
  *
  * @param table         the indexed table
  * @param indexMetadata the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class IndexService(val table: ColumnFamilyStore, val indexMetadata: IndexMetadata)
  extends IndexServiceMBean {

  val metadata = table.metadata
  val ksName = metadata.ksName
  val cfName = metadata.cfName
  val name = indexMetadata.name
  val column = indexedColumn(indexMetadata)
  val columnDefinition = getColumnDefinition(metadata, column)
  val qualifiedName = s"$ksName.$cfName.$name"

  // Parse options
  val options = new IndexOptions(metadata, indexMetadata)

  // Setup mapping
  val schema = options.schema
  val tokenMapper = new TokenMapper
  val partitionMapper = new PartitionMapper(metadata)
  val regularCells = metadata.partitionColumns.regulars
  val mappedRegularCells = regularCells.map(_.name.toString).filter(schema.mappedCells.contains)
  val mapsMultiCells = regularCells
    .exists(x => x.`type`.isMultiCell && schema.mapsCell(x.name.toString))

  // Setup FS index and write queue
  val queue = TaskQueue.build(options.indexingThreads, options.indexingQueuesSize)
  val lucene = new FSIndex(
    name,
    options.path,
    options.schema.analyzer,
    options.refreshSeconds,
    options.ramBufferMB,
    options.maxMergeMB,
    options.maxCachedMB)

  // Delay JMX MBean creation
  var mBean: ObjectName = _

  def init() {

    // Initialize index
    try {
      val sort = new Sort(keySortFields.toArray: _*)
      lucene.init(sort, fieldsToLoad)
    } catch {
      case e: Exception => logger
        .error(s"Initialization of Lucene FS directory for index '$name' has failed:", e)
    }

    // Register JMX MBean
    try {
      val mBeanName = "com.stratio.cassandra.lucene:type=Lucene," +
        s"keyspace=$ksName,table=$cfName,index=$name"
      mBean = new ObjectName(mBeanName)
      ManagementFactory.getPlatformMBeanServer.registerMBean(this, this.mBean)
    } catch {
      case e: JMException => logger.error("Error while registering Lucene index JMX MBean", e)
    }
  }

  /** Returns the Lucene [[SortField]]s required to retrieve documents sorted by primary key.
    *
    * @return the sort fields
    */
  def keySortFields: List[SortField]

  /** Returns the names of the Lucene fields to be loaded from index during searches.
    *
    * @return the names of the fields to be loaded
    */
  def fieldsToLoad: Set[String]

  /** Returns a [[Columns]] representing the specified row.
    *
    * @param key the partition key
    * @param row the row
    * @return the columns representing the specified row
    */
  def columns(key: DecoratedKey, row: Row): Columns

  def keyIndexableFields(key: DecoratedKey, row: Row): List[IndexableField]

  /** Returns if the specified column definition is mapped by this index.
    *
    * @param columnDef a column definition
    * @return `true` if the column is mapped, `false` otherwise
    */
  def dependsOn(columnDef: ColumnDefinition): Boolean = {
    schema.mapsCell(columnDef.name.toString)
  }

  /** Returns if the specified expression is targeted to this index
    *
    * @param expression a CQL query expression
    * @return `true` if `expression` is targeted to this index, `false` otherwise
    */
  def supportsExpression(expression: RowFilter.Expression): Boolean = {
    supportsExpression(expression.column, expression.operator)
  }

  /** Returns if a CQL expression with the specified column definition and operator is targeted to
    * this index.
    *
    * @param columnDef the expression column definition
    * @param operator  the expression operator
    * @return `true` if the expression is targeted to this index, `false` otherwise
    */
  def supportsExpression(columnDef: ColumnDefinition, operator: Operator): Boolean = {
    operator == Operator.EQ && column.contains(columnDef.name.toString)
  }

  /** Returns a copy of the specified [[RowFilter]] without any Lucene expressions.
    *
    * @param filter a row filter
    * @return a copy of `filter` without Lucene expressions
    */
  def getPostIndexQueryFilter(filter: RowFilter): RowFilter = {
    if (column.isEmpty) return filter
    filter.foldLeft(filter)((f, e) => if (supportsExpression(e)) f.without(e) else f)
  }

  /** Returns the validated search contained in the specified expression.
    *
    * @param expression a custom CQL expression
    * @return the validated expression
    */
  def validate(expression: RowFilter.Expression): Search = {
    val value = expression match {
      case c: CustomExpression => c.getValue
      case _ => expression.getIndexValue
    }
    val json = UTF8Type.instance.compose(value)
    val search = SearchBuilder.fromJson(json).build
    search.validate(schema)
  }

  /** Returns the Lucene document representing the specified row. Only the fields required by the
    * post processing phase of the specified search will be added.
    *
    * @param key    the partition key
    * @param row    the row
    * @param search a search
    * @return a document
    */
  def document(key: DecoratedKey, row: Row, search: Search): Document = {
    val doc = new Document
    val cols = columns(key, row)
    keyIndexableFields(key, row).foreach(doc.add)
    schema.postProcessingIndexableFields(cols, search).foreach(doc.add)
    doc
  }

  /** Returns a Lucene term uniquely identifying the specified row.
    *
    * @param key the partition key
    * @param row the row
    * @return a Lucene identifying term
    */
  def term(key: DecoratedKey, row: Row): Term

  /** Returns a Lucene term identifying documents representing all the row's which are in the
    * partition the specified [[DecoratedKey]].
    *
    * @param key the partition key
    * @return a Lucene term representing `key`
    */
  def term(key: DecoratedKey): Term = {
    partitionMapper.term(key)
  }

  /** Returns if SSTables can contain additional columns of the specified row so read-before-write
    * is required prior to indexing.
    *
    * @param key the partition key
    * @param row the row
    * @return `true` if read-before-write is required, `false` otherwise
    */
  def needsReadBeforeWrite(key: DecoratedKey, row: Row): Boolean = {
    mapsMultiCells || !row.columns.map(_.name.toString).containsAll(mappedRegularCells)
  }

  /** Returns the [[DecoratedKey]] contained in the specified Lucene document.
    *
    * @param document the document containing the partition key to be get
    * @return the partition key contained in the specified Lucene document
    */
  def decoratedKey(document: Document): DecoratedKey = {
    partitionMapper.decoratedKey(document)
  }

  /** Creates an new [[IndexWriter]] object for updates to a given partition.
    *
    * @param key             key of the partition being modified
    * @param nowInSec        current time of the update operation
    * @param orderGroup      operation group spanning the update operation
    * @param transactionType what kind of update is being performed on the base data
    * @return the newly created index writer
    */
  def writer(
      key: DecoratedKey,
      nowInSec: Int,
      orderGroup: OpOrder.Group,
      transactionType: IndexTransaction.Type): IndexWriter

  /** Deletes all the index contents. */
  def truncate() {
    queue.submitSynchronous(lucene.truncate)
  }

  /** Closes and removes all the index files. */
  def delete() {
    try {
      queue.close()
      ManagementFactory.getPlatformMBeanServer.unregisterMBean(mBean)
    } catch {
      case e: JMException => logger.error("Error while unregistering Lucene index MBean", e)
    } finally {
      lucene.delete()
    }
  }

  /** Upserts the specified row.
    *
    * @param key      the partition key
    * @param row      the row to be upserted
    * @param nowInSec now in seconds
    */
  def upsert(key: DecoratedKey, row: Row, nowInSec: Int) {
    queue.submitAsynchronous(
      key, () => {
        val t = term(key, row)
        val cols = columns(key, row).withoutDeleted(nowInSec)
        val fields = schema.indexableFields(cols)
        if (fields.isEmpty) {
          lucene.delete(t)
        } else {
          val doc = new Document()
          keyIndexableFields(key, row).foreach(doc.add)
          fields.foreach(doc.add)
          lucene.upsert(t, doc)
        }
      })
  }

  /** Deletes the partition identified by the specified key.
    *
    * @param key the partition key
    * @param row the row to be deleted
    */
  def delete(key: DecoratedKey, row: Row) {
    queue.submitAsynchronous(key, () => lucene.delete(term(key, row)))
  }

  /** Deletes the partition identified by the specified key.
    *
    * @param key the partition key
    */
  def delete(key: DecoratedKey) {
    queue.submitAsynchronous(key, () => lucene.delete(term(key)))
  }

  /** Returns a new index searcher for the specified read command.
    *
    * @param command the read command being executed
    * @return a searcher with which to perform the supplied command
    */
  def search(command: ReadCommand, orderGroup: ReadOrderGroup): UnfilteredPartitionIterator = {

    // Parse search
    Tracer.trace("Building Lucene search")
    val expr = expression(command)
    val search = SearchBuilder.fromJson(expr).build
    val q = search.query(schema, query(command).orNull)
    val a = after(search.paging, command)
    val s = sort(search)
    val n = command.limits.count

    // Refresh if required
    if (search.refresh) {
      Tracer.trace("Refreshing Lucene index searcher")
      refresh()
    }

    // Search
    Tracer.trace(s"Lucene index searching for $n rows")
    val documents = lucene.search(a, q, s, n)
    reader(documents, command, orderGroup)
  }

  def search(command: ReadCommand): Search = {
    SearchBuilder.fromJson(expression(command)).build
  }

  def search(group: SinglePartitionReadCommand.Group): Search = {
    SearchBuilder.fromJson(expression(group)).build
  }

  def expression(command: ReadCommand): String = {
    command.rowFilter.getExpressions.collect {
      case e: CustomExpression if name == e.getTargetIndex.name => e.getValue
      case e if supportsExpression(e) => e.getIndexValue
    }.map(UTF8Type.instance.compose).head
  }

  def expression(group: SinglePartitionReadCommand.Group): String = {
    expression(group.commands.head)
  }

  /** Returns the key range query represented by the specified read command.
    *
    * @param command the read command
    * @return the key range query
    */
  def query(command: ReadCommand): Option[Query] = command match {
    case command: SinglePartitionReadCommand =>
      val key = command.partitionKey
      val filter = command.clusteringIndexFilter(key)
      Some(query(key, filter))
    case command: PartitionRangeReadCommand => query(command.dataRange)
    case _ => throw new IndexException(s"Unsupported read command ${command.getClass}")
  }

  /** Returns a query to get the documents satisfying the specified key and clustering filter.
    *
    * @param key    the partition key
    * @param filter the clustering key range
    * @return a query to get the documents satisfying the key range
    */
  def query(key: DecoratedKey, filter: ClusteringIndexFilter): Query

  /** Returns a query to get the documents satisfying the specified data range.
    *
    * @param dataRange the data range
    * @return a query to get the documents satisfying the data range
    */
  def query(dataRange: DataRange): Option[Query]

  def after(pagingState: IndexPagingState, command: ReadCommand): Option[Term] = {
    if (pagingState == null) return None
    pagingState.forCommand(command).map { case (key, clustering) => after(key, clustering) }
  }

  /** Returns a Lucene query to retrieve the row identified by the specified paging state.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return the query to retrieve the row
    */
  def after(key: DecoratedKey, clustering: Clustering): Term

  /** Returns the Lucene sort with the specified search sorting requirements followed by the
    * Cassandra's natural ordering based on partitioning token and cell name.
    *
    * @param search the search containing sorting requirements
    * @return a Lucene sort according to `search`
    */
  def sort(search: Search): Sort = {
    val sortFields = mutable.ListBuffer[SortField]()
    if (search.usesSorting) {
      sortFields.addAll(search.sortFields(schema))
    }
    if (search.usesRelevance) {
      sortFields.add(SortField.FIELD_SCORE)
    }
    sortFields.addAll(keySortFields)
    new Sort(sortFields.toArray: _*)
  }

  /** Retrieves from the local storage the rows in the specified partition slice.
    *
    * @param key         the partition key
    * @param clusterings the clustering keys
    * @param nowInSec    max allowed time in seconds
    * @param group       operation group spanning the calling operation
    * @return a row iterator
    */
  def read(
      key: DecoratedKey,
      clusterings: java.NavigableSet[Clustering],
      nowInSec: Int,
      group: OpOrder.Group): UnfilteredRowIterator = {
    val filter = new ClusteringIndexNamesFilter(clusterings, false)
    val columnFilter = ColumnFilter.all(metadata)
    SinglePartitionReadCommand.create(metadata, nowInSec, key, columnFilter, filter)
      .queryMemtableAndDisk(table, group)
  }

  /** Retrieves from the local storage all the rows in the specified partition.
    *
    * @param key      the partition key
    * @param nowInSec max allowed time in seconds
    * @param opGroup  operation group spanning the calling operation
    * @return a row iterator
    */
  def read(key: DecoratedKey, nowInSec: Int, opGroup: OpOrder.Group): UnfilteredRowIterator = {
    val clusterings = new java.TreeSet[Clustering](metadata.comparator)
    clusterings.add(Clustering.EMPTY)
    read(key, clusterings, nowInSec, opGroup)
  }

  /** Reads from the local SSTables the rows identified by the specified search.
    *
    * @param documents  the Lucene documents
    * @param command    the Cassandra command
    * @param orderGroup the Cassandra read order group
    * @return the local rows satisfying the search
    */
  def reader(
      documents: DocumentIterator,
      command: ReadCommand,
      orderGroup: ReadOrderGroup): IndexReader

  /** Post processes in the coordinator node the results of a distributed search.  In other words,
    * gets the k globally best results from all the k best node-local results.
    *
    * @param partitions the node results iterator
    * @param group      the read command group
    * @return the k globally best results
    */
  def postProcess(
      partitions: PartitionIterator,
      group: SinglePartitionReadCommand.Group): PartitionIterator = {
    if (group.commands.size <= 1) return partitions //Only one partition is involved
    postProcess(partitions, search(group), group.limits.count, group.nowInSec)
  }

  /** Post processes in the coordinator node the results of a distributed search. In other words,
    * gets the k globally best results from all the k best node-local results.
    *
    * @param partitions the node results iterator
    * @param command    the read command
    * @return the k globally best results
    */
  def postProcess(
      partitions: PartitionIterator,
      command: ReadCommand): PartitionIterator = command match {
    case c: SinglePartitionReadCommand => partitions
    case _ => postProcess(partitions, search(command), command.limits.count, command.nowInSec)
  }

  def postProcess(
      partitions: PartitionIterator,
      search: Search,
      limit: Int,
      nowInSec: Int): PartitionIterator = {
    if (search.requiresFullScan) {
      val rows = collect(partitions)
      if (search.requiresPostProcessing && rows.nonEmpty) {
        return merge(search, limit, nowInSec, rows)
      }
    }
    partitions
  }

  def collect(partitions: PartitionIterator): Seq[(DecoratedKey, SimpleRowIterator)] = {
    val rows = new java.LinkedList[(DecoratedKey, SimpleRowIterator)]
    val time = TimeCounter.create.start
    for (partition <- partitions) {
      try {
        val key = partition.partitionKey
        while (partition.hasNext) {
          rows.add((key, new SimpleRowIterator(partition)))
        }
      } finally partition.close()
    }
    logger.debug(s"Collected ${rows.size} rows in ${time.stop}")
    rows
  }

  def merge(
      search: Search,
      limit: Int,
      nowInSec: Int,
      rows: Seq[(DecoratedKey, SimpleRowIterator)]): PartitionIterator = {

    val time = TimeCounter.create.start
    val field = "_id"
    val index = new RAMIndex(schema.analyzer)
    try {

      // Index collected rows in memory
      for (id <- rows.indices) {
        val (key, rowIterator) = rows(id)
        val row = rowIterator.row
        val doc = document(key, row, search)
        doc.add(new StoredField(field, id)) // Mark document
        index.add(doc)
      }

      // Repeat search to sort partial results
      val docs = index.search(search.postProcessingQuery(schema), sort(search), limit, Set(field))

      // Collect and decorate
      val merged = for ((doc, score) <- docs) yield {
        val id = doc.get(field).toInt
        val rowIterator = rows.get(id)._2
        rowIterator.decorated(row => decorate(row, score, nowInSec))
      }

      Tracer.trace(s"Lucene post-process ${rows.size} collected rows to ${merged.size} rows")
      logger.debug(s"Post-processed ${rows.size} rows to ${merged.size} rows in ${time.stop}")
      new SimplePartitionIterator(merged)

    } finally index.close()
  }

  def decorate(row: Row, score: ScoreDoc, nowInSec: Int): Row = {

    // Skip if there is no base column or score
    if (columnDefinition.isEmpty) return row

    // Copy row
    val builder = BTreeRow.unsortedBuilder(nowInSec)
    builder.newRow(row.clustering)
    builder.addRowDeletion(row.deletion)
    builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo)
    row.cells.foreach(builder.addCell)

    // Add score cell
    val timestamp = row.primaryKeyLivenessInfo.timestamp
    val scoreCellValue = UTF8Type.instance.decompose(score.score.toString)
    builder.addCell(BufferCell.live(metadata, columnDefinition.get, timestamp, scoreCellValue))

    builder.build
  }

  /** Ensures that values present in a partition update are valid according to the schema.
    *
    * @param update the partition update containing the values to be validated
    */
  def validate(update: PartitionUpdate) {
    val key = update.partitionKey
    update.foreach(row => schema.validate(columns(key, row)))
  }

  /** @inheritdoc*/
  override def commit() {
    queue.submitSynchronous(lucene.commit)
  }

  /** @inheritdoc*/
  override def getNumDocs: Int = {
    lucene.getNumDocs
  }

  /** @inheritdoc*/
  override def getNumDeletedDocs: Int = {
    lucene.getNumDeletedDocs
  }

  /** @inheritdoc*/
  override def forceMerge(maxNumSegments: Int, doWait: Boolean) {
    queue.submitSynchronous(() => lucene.forceMerge(maxNumSegments, doWait))
  }

  /** @inheritdoc*/
  override def forceMergeDeletes(doWait: Boolean) {
    queue.submitSynchronous(() => lucene.forceMergeDeletes(doWait))
  }

  /** @inheritdoc*/
  override def refresh() {
    queue.submitSynchronous(lucene.refresh)
  }

}

object IndexService {

  val logger = LoggerFactory.getLogger(classOf[IndexService])

  /** Returns a new index service for the specified indexed table and index metadata.
    *
    * @param table         the indexed table
    * @param indexMetadata the index metadata
    * @return the index service
    */
  def build(table: ColumnFamilyStore, indexMetadata: IndexMetadata): IndexService = {
    if (table.getComparator.subtypes.isEmpty) {
      new IndexServiceSkinny(table, indexMetadata)
    } else {
      new IndexServiceWide(table, indexMetadata)
    }
  }

  def indexedColumn(indexMetadata: IndexMetadata): Option[String] = {
    Option(indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME)).filterNot(StringUtils.isBlank)
  }

  def getColumnDefinition(metadata: CFMetaData, name: Option[String]): Option[ColumnDefinition] = {
    name.flatMap(name => metadata.allColumns.find(_.name.toString == name))
  }

}