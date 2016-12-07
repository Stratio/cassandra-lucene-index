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
package com.stratio.cassandra.lucene.index

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc, Sort}
import org.apache.lucene.store.RAMDirectory

/** Class wrapping a Lucene RAM directory and its readers, writers and searchers for NRT.
  *
  * @param analyzer the index writer analyzer
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class RAMIndex(analyzer: Analyzer) {

  private val directory = new RAMDirectory
  private val indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer))

  /** Adds the specified document.
    *
    * @param document the document to be added
    */
  def add(document: Document) {
    indexWriter.addDocument(document)
  }

  /** Commits all pending changes to the index, waits for pending merges to complete, and closes all
    * associated resources.
    */
  def close() {
    indexWriter.close()
    directory.close()
  }

  /** Finds the top count hits for a query and a sort.
    *
    * @param query  the query to search for
    * @param sort   the sort to be applied
    * @param count  the max number of results to be collected
    * @param fields the names of the fields to be loaded
    * @return the found documents
    */
  def search(
      query: Query,
      sort: Sort,
      count: Integer,
      fields: java.util.Set[String]): Seq[(Document, ScoreDoc)] = {
    indexWriter.commit()
    val reader = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    try {
      val newSort = sort.rewrite(searcher)
      val topDocs = searcher.search(query, count, newSort, true, true)
      topDocs.scoreDocs.map(score => (searcher.doc(score.doc, fields), score))
    } finally searcher.getIndexReader.close()
  }

}
