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

import org.apache.cassandra.db.rows.Row
import org.apache.cassandra.db.{Clustering, DecoratedKey}
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.index.transactions.IndexTransaction.Type._
import org.apache.cassandra.utils.concurrent.OpOrder

import scala.collection.JavaConverters._


/** [[IndexWriter]] for wide rows.
  *
  * @param service         the service to perform the indexing operation
  * @param key             key of the partition being modified
  * @param nowInSec        current time of the update operation
  * @param opGroup         operation group spanning the update operation
  * @param transactionType what kind of update is being performed on the base data
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexWriterWide(
    service: IndexServiceWide,
    key: DecoratedKey,
    nowInSec: Int,
    opGroup: OpOrder.Group,
    transactionType: IndexTransaction.Type)
  extends IndexWriter(service, key, nowInSec, opGroup, transactionType) {

  /** The clustering keys of the rows needing read before write. */
  private val clusterings = new java.util.TreeSet[Clustering](metadata.comparator)

  /** The rows ready to be written. */
  private val rows = new java.util.TreeMap[Clustering, Row](metadata.comparator)

  /** @inheritdoc */
  override def delete() {
    service.delete(key)
    clusterings.clear()
    rows.clear()
  }

  /** @inheritdoc */
  override def index(row: Row) {
    if (!row.isStatic) {
      val clustering = row.clustering
      if (service.needsReadBeforeWrite(key, row)) {
        tracer.trace("Lucene index doing read before write")
        clusterings.add(clustering)
      } else {
        tracer.trace("Lucene index skipping read before write")
        rows.put(clustering, row)
      }
    }
  }

  /** @inheritdoc */
  override def finish() {

    // Skip on cleanups
    if (transactionType == CLEANUP) return

    // Read required rows from storage engine
    read(key, clusterings)
      .asScala
      .map(_.asInstanceOf[Row])
      .foreach(row => rows.put(row.clustering(), row))

    // Write rows
    rows.forEach((clustering, row) => {
      if (row.hasLiveData(nowInSec)) {
        tracer.trace("Lucene index writing document")
        service.upsert(key, row, nowInSec)
      } else {
        tracer.trace("Lucene index deleting document")
        service.delete(key, clustering)
      }
    })
  }

}