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

import java.{util => java}

import com.stratio.cassandra.lucene.index.DocumentIterator
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter
import org.apache.lucene.document.Document

/** [[IndexReader]] for wide rows.
  *
  * @param service    the index service
  * @param command    the read command
  * @param table      the base table
  * @param controller the read execution controller
  * @param documents  the documents iterator
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexReaderWide(
    service: IndexServiceWide,
    command: ReadCommand,
    table: ColumnFamilyStore,
    controller: ReadExecutionController,
    documents: DocumentIterator)
  extends IndexReader(command, table, controller, documents) {

  private[this] val comparator = service.metadata.comparator
  private[this] var nextDoc: Document = _

  private[this] def readClusterings(key: DecoratedKey): java.NavigableSet[Clustering] = {
    val clusterings = new java.TreeSet[Clustering](comparator)
    var clustering = service.clustering(nextDoc)
    var lastClustering: Clustering = null
    var continue = true
    while (continue && nextDoc != null && key.getKey == service.decoratedKey(nextDoc).getKey &&
      (lastClustering == null || comparator.compare(lastClustering, clustering) < 0)) {
      if (command.selectsKey(key) && command.selectsClustering(key, clustering)) {
        lastClustering = clustering
        clusterings.add(clustering)
      }
      if (documents.hasNext) {
        nextDoc = documents.next._1
        clustering = service.clustering(nextDoc)
      }
      else nextDoc = null
      continue = !documents.needsFetch
    }
    clusterings
  }

  /** @inheritdoc */
  override protected def prepareNext(): Boolean = {

    if (nextData.isDefined) return true

    if (nextDoc == null) {
      if (!documents.hasNext) return false
      nextDoc = documents.next._1
    }

    val key = service.decoratedKey(nextDoc)
    val clusterings = readClusterings(key)

    if (clusterings.isEmpty) return prepareNext()

    val filter = new ClusteringIndexNamesFilter(clusterings, false)
    nextData = Some(read(key, filter))

    nextData.foreach(
      data => if (data.isEmpty) {
        data.close()
        return prepareNext()
      })

    true
  }


}
