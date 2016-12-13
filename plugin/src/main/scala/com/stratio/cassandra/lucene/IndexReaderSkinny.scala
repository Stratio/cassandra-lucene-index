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

import com.stratio.cassandra.lucene.index.DocumentIterator
import org.apache.cassandra.db._

/** [[IndexReader]] for skinny rows.
  *
  * @param service    the index service
  * @param command    the read command
  * @param table      the base table
  * @param controller the read execution controller
  * @param documents  the documents iterator
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexReaderSkinny(
    service: IndexServiceSkinny,
    command: ReadCommand,
    table: ColumnFamilyStore,
    controller: ReadExecutionController,
    documents: DocumentIterator)
  extends IndexReader(command, table, controller, documents) {

  /** @inheritdoc */
  override protected def prepareNext(): Boolean = {
    while (nextData.isEmpty && documents.hasNext) {
      val nextDoc = documents.next
      val key = service.decoratedKey(nextDoc._1)
      val filter = command.clusteringIndexFilter(key)
      nextData = Some(read(key, filter))
      nextData.foreach(d => if (d.isEmpty) d.close())
    }
    nextData.isDefined
  }

}
