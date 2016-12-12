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

/** Tests for [[PartitionedIndex]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionedIndexTest extends BaseScalaTest {

  val REFRESH_SECONDS: Double = 0.1D
  val REFRESH_MILLISECONDS: Int = (REFRESH_SECONDS * 1000).toInt
  val WAIT_MILLISECONDS: Int = REFRESH_MILLISECONDS * 2

  def assertCount(docs: DocumentIterator, expected: Int) {
    var count = 0
    docs.foreach(_ => count += 1)
    assertEquals("Expected " + expected + " documents", expected, count)
  }

  def doWithIndex(numPartitions: Int, f: PartitionedIndex => Unit): Unit = {
    val temporaryFolder = new TemporaryFolder
    temporaryFolder.create()
    try {
      val index = new PartitionedIndex(
        numPartitions,
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

  test("CRUD without partitioning") {
    doWithIndex(1, index => {
      val sort = new Sort(new SortedSetSortField("field", false))
      val fields = Collections.singleton("field")
      index.init(sort, fields)

      assertEquals("Index must be empty", 0, index.getNumDocs)

      val term1 = new Term("field", "value1")
      val document1 = new Document
      document1.add(new StringField("field", "value1", Field.Store.NO))
      document1.add(new SortedSetDocValuesField("field", new BytesRef("value1")))
      index.upsert(0, term1, document1)

      val term2 = new Term("field", "value2")
      val document2 = new Document
      document2.add(new StringField("field", "value2", Field.Store.NO))
      document2.add(new SortedSetDocValuesField("field", new BytesRef("value2")))
      document2.add(new SortedSetDocValuesField("field", new BytesRef("value3")))
      index.upsert(0, term2, document2)

      index.commit()
      Thread.sleep(REFRESH_MILLISECONDS)
      assertEquals("Expected 2 documents", 2, index.getNumDocs)

      val query = new WildcardQuery(new Term("field", "value*"))
      assertCount(index.search(List(0), None, query, sort, 1), 2)

      // Delete by term
      index.delete(0, term1)
      index.commit()
      Thread.sleep(WAIT_MILLISECONDS)
      assertEquals("Expected 1 document", 1, index.getNumDocs)

      // Upsert
      index.upsert(0, term1, document1)
      index.upsert(0, term2, document2)
      index.upsert(0, term2, document2)
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

  test("CRUD with partitioning") {
    doWithIndex(2, index => {
      val sort = new Sort(new SortedSetSortField("field", false))
      val fields = Collections.singleton("field")
      index.init(sort, fields)

      assertEquals("Index must be empty", 0, index.getNumDocs)

      val term1 = new Term("field", "value1")
      val document1 = new Document
      document1.add(new StringField("field", "value1", Field.Store.NO))
      document1.add(new SortedSetDocValuesField("field", new BytesRef("value1")))
      index.upsert(0, term1, document1)

      val term2 = new Term("field", "value2")
      val document2 = new Document
      document2.add(new StringField("field", "value2", Field.Store.NO))
      document2.add(new SortedSetDocValuesField("field", new BytesRef("value2")))
      document2.add(new SortedSetDocValuesField("field", new BytesRef("value3")))
      index.upsert(1, term2, document2)

      index.commit()
      Thread.sleep(REFRESH_MILLISECONDS)
      assertEquals("Expected 2 documents", 2, index.getNumDocs)

      val query = new WildcardQuery(new Term("field", "value*"))
      assertCount(index.search(List(0), None, query, sort, 1), 1)
      assertCount(index.search(List(1), None, query, sort, 1), 1)
      assertCount(index.search(List(0, 1), None, query, sort, 1), 2)

      // Delete by term
      index.delete(0, term1)
      index.commit()
      Thread.sleep(WAIT_MILLISECONDS)
      assertEquals("Expected 1 document", 1, index.getNumDocs)

      // Upsert
      index.upsert(0, term1, document1)
      index.upsert(1, term2, document2)
      index.upsert(1, term2, document2)
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

  test("pagination without partitioning") {
    doWithIndex(1, index => {
      val sort = new Sort(new SortedNumericSortField("field", SortField.Type.INT, false))
      val fields = Collections.singleton("field")
      index.init(sort, fields)

      assertEquals("Index must be empty", 0, index.getNumDocs)

      for (i <- 0 until 100) {
        val value = i.toString
        val term = new Term("field_s", value)
        val document = new Document
        document.add(new StringField("field_s", value, Field.Store.NO))
        document.add(new SortedNumericDocValuesField("field", i))
        index.upsert(0, term, document)
      }

      index.commit()
      Thread.sleep(REFRESH_MILLISECONDS)
      assertEquals("Expected 2 documents", 100, index.getNumDocs)
      val query = new MatchAllDocsQuery
      assertCount(index.search(List(0), None, query, sort, 1000), 100)
      assertCount(index.search(List(0), Some(new Term("field_s", "49")), query, sort, 1000), 50)
    })
  }

  test("pagination with partitioning") {
    doWithIndex(2, index => {
      val sort = new Sort(new SortedNumericSortField("field", SortField.Type.INT, false))
      val fields = Collections.singleton("field")
      index.init(sort, fields)

      assertEquals("Index must be empty", 0, index.getNumDocs)

      for (i <- 0 until 100) {
        val value = i.toString
        val term = new Term("field_s", value)
        val document = new Document
        document.add(new StringField("field_s", value, Field.Store.NO))
        document.add(new SortedNumericDocValuesField("field", i))
        index.upsert(i % 2, term, document)
      }

      index.commit()
      Thread.sleep(REFRESH_MILLISECONDS)
      assertEquals("Expected 2 documents", 100, index.getNumDocs)
      val query = new MatchAllDocsQuery
      assertCount(index.search(List(0, 1), None, query, sort, 1000), 100)
      assertCount(index.search(List(0, 1), Some(new Term("field_s", "49")), query, sort, 1000), 50)
    })
  }

}