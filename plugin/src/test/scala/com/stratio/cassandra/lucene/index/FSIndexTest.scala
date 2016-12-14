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

import java.nio.file.Paths
import java.util.{Collections, UUID}

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.IndexOptions._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef
import org.junit.Assert.assertEquals
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[FSIndex]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class FSIndexTest extends BaseScalaTest {

  val REFRESH_SECONDS: Double = 0.1D
  val REFRESH_MILLISECONDS: Int = (REFRESH_SECONDS * 1000).toInt
  val WAIT_MILLISECONDS: Int = REFRESH_MILLISECONDS * 2

  def assertCount(docs: DocumentIterator, expected: Int) {
    var count = 0
    docs.foreach(_ => count += 1)
    assertEquals("Expected " + expected + " documents", expected, count)
  }

  def doWithIndex(f: FSIndex => Unit): Unit = {
    val temporaryFolder = new TemporaryFolder
    temporaryFolder.create()
    try {
      val index = new FSIndex(
        "test_index",
        Paths.get(temporaryFolder.newFolder("directory" + UUID.randomUUID).getPath),
        new StandardAnalyzer,
        REFRESH_SECONDS,
        DEFAULT_RAM_BUFFER_MB,
        DEFAULT_MAX_MERGE_MB,
        DEFAULT_MAX_CACHED_MB)
      f.apply(index)
    } finally temporaryFolder.delete()
  }

  test("CRUD operations") {
    doWithIndex(
      index => {
        val sort = new Sort(new SortedSetSortField("field", false))
        val fields = Collections.singleton("field")
        index.init(sort, fields)

        assertEquals("Index must be empty", 0, index.getNumDocs)

        val term1 = new Term("field", "value1")
        val document1 = new Document
        document1.add(new StringField("field", "value1", Field.Store.NO))
        document1.add(new SortedSetDocValuesField("field", new BytesRef("value1")))
        index.upsert(term1, document1)

        val term2 = new Term("field", "value2")
        val document2 = new Document
        document2.add(new StringField("field", "value2", Field.Store.NO))
        document2.add(new SortedSetDocValuesField("field", new BytesRef("value2")))
        document2.add(new SortedSetDocValuesField("field", new BytesRef("value3")))
        index.upsert(term2, document2)

        index.commit()
        Thread.sleep(REFRESH_MILLISECONDS)
        assertEquals("Expected 2 documents", 2, index.getNumDocs)

        // Delete by term
        index.delete(term1)
        index.commit()
        Thread.sleep(WAIT_MILLISECONDS)
        assertEquals("Expected 1 document", 1, index.getNumDocs)

        // Upsert
        index.upsert(term1, document1)
        index.upsert(term2, document2)
        index.upsert(term2, document2)
        index.commit()
        Thread.sleep(WAIT_MILLISECONDS)
        assertEquals("Expected 2 documents", 2, index.getNumDocs)

        // Truncate
        index.truncate()
        index.commit()
        Thread.sleep(WAIT_MILLISECONDS)
        assertEquals("Expected 0 documents", 0, index.getNumDocs)

        // Delete
        index.delete()
      })
  }

}
