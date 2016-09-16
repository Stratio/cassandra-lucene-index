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
package com.stratio.cassandra.lucene.partitioning

import java.io.Reader
import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.partitioning.Partitioner.{Decorator, PartitionerDecorator}
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.cql3.Operator
import org.apache.cassandra.db.filter.RowFilter
import org.apache.cassandra.db.filter.RowFilter.Expression
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.db.{DecoratedKey, ReadCommand, SinglePartitionReadCommand}
import org.apache.cassandra.utils.MurmurHash
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.document.{Document, Field, FieldType, StringField}
import org.apache.lucene.index._
import org.apache.lucene.search.BooleanClause.Occur._
import org.apache.lucene.search.{BooleanQuery, MatchAllDocsQuery, Query}
import org.apache.lucene.util.BytesRef
import org.codehaus.jackson.annotate.{JsonCreator, JsonProperty}

import scala.collection.JavaConversions._

/**
  * @author Andres de la Pena { @literal <adelapena@stratio.com>}
  */
case class Partitioner(metadata: CFMetaData, numPartitions: Int, column: String) {

  if (numPartitions < 1)
    throw new IndexException("The number of partitions should be strictly positive")

  if (column == null || column.isEmpty)
    throw new IndexException("Partition column should be defined")

  if (!metadata.allColumns().exists(_.name.toString == column))
    throw new IndexException(s"Partition column $column doesn't exist")

  val columnDefinition = metadata.partitionKeyColumns().find(_.name.toString == column)
    .getOrElse(throw new IndexException(s"Partition column '$column' isn't a partition key column"))


  val fieldType: FieldType = new FieldType
  fieldType.setOmitNorms(true)
  fieldType.setIndexOptions(IndexOptions.DOCS)
  fieldType.setTokenized(false)
  fieldType.setStored(false)
  fieldType.setDocValuesType(DocValuesType.SORTED)
  fieldType.freeze()

  val validator = columnDefinition.cellValueType()

  val partitionKeyType = metadata.getKeyValidator

  val expressionValueField = classOf[Expression].getDeclaredField("value")
  expressionValueField.setAccessible(true)

  def partition(value: ByteBuffer): Int = {
    val murmur: Array[Long] = new Array[Long](2)
    MurmurHash.hash3_x64_128(value, value.position, value.remaining, 0, murmur)
    Math.abs(murmur(0) % numPartitions).toInt
  }

  def value(key: DecoratedKey): ByteBuffer = {
    val values = metadata.getKeyValidator match {
      case c: CompositeType => c.split(key.getKey)
      case _ => Array[ByteBuffer](key.getKey)
    }
    values(columnDefinition.position)
  }

  def value(filter: RowFilter): ByteBuffer = {
    filter.getExpressions
      .find(x => x.operator() == Operator.EQ && x.column() == columnDefinition)
      .map(expressionValueField.get(_).asInstanceOf[ByteBuffer])
      .getOrElse(throw new IndexException("PARTITION"))
  }

  def partition(key: DecoratedKey): Int = {
    partition(value(key))
  }

  //  def query(command: ReadCommand): Option[(Int, Option[Query])] = command match {
  //    case c: SinglePartitionReadCommand =>
  //      val _value = value(c.partitionKey())
  //      val _partition = partition(_value)
  //      Some(_partition, None)
  //    case _ =>
  //      value(command.rowFilter()) match {
  //        case Some(_value) =>
  //          val _partition = partition(_value)
  //          val bytes = ByteBufferUtils.bytesRef(_value)
  //          val term = new Term(fieldName, bytes)
  //          val query = new TermQuery(term)
  //          Some(_partition, Some(query))
  //        case None => None
  //      }
  //  }

  def query(command: ReadCommand): Decorator = command match {
    case c: SinglePartitionReadCommand =>
      val _value = value(c.partitionKey())
      val _partition = partition(_value)
      PartitionerDecorator(_value, _partition)
    case _ =>
      val _value = value(command.rowFilter())
      val _partition = partition(_value)
      PartitionerDecorator(_value, _partition)
  }

  def decorator(key: DecoratedKey): Decorator = {
    val _value = value(key)
    val _partition = partition(_value)
    PartitionerDecorator(_value, _partition)
  }

}

object Partitioner {

  val fieldName = "_partition"

  val NOP_DECORATOR = new NopDecorator

  @JsonCreator
  case class Builder(@JsonProperty("partitions") numPartitions: Int, @JsonProperty("column") column: String) {
    def build(metadata: CFMetaData): Partitioner = Partitioner(metadata, numPartitions, column)
  }

  def decorate(name: String, partition: Int): String = name match {
    case s if s.startsWith("_") => s"_${partition}_${s.stripPrefix("_")}"
    case _ => s"_${partition}_$name"
  }

  case class DecoratedIndexableField(field: IndexableField, partition: Int) extends IndexableField {

    def name: String = s"_${partition}_${field.name()}"

    def fieldType: IndexableFieldType = field.fieldType()

    def boost: Float = field.boost()

    def binaryValue: BytesRef = field.binaryValue()

    def stringValue: String = field.stringValue()

    def readerValue: Reader = field.readerValue()

    def numericValue: Number = field.numericValue()

    def tokenStream(analyzer: Analyzer, reuse: TokenStream): TokenStream = field.tokenStream(analyzer, reuse)
  }

  abstract class Decorator() {

    def decorate(term: Term): Term

    def decorate(name: String): String

    def decorate(document: Document)

    def decorate(query: Query): Query
  }

  case class PartitionerDecorator(value: ByteBuffer, partition: Int) extends Decorator {

    lazy val bytes = ByteBufferUtils.bytesRef(value)

    def decorate(term: Term): Term = new Term(decorate(term.field()), term.bytes())

    def decorate(name: String): String = s"${name}_$partition"

    def decorate(document: Document) = document.add(new StringField(fieldName, bytes, Field.Store.NO))

    def decorate(query: Query): Query = query match {
      case b: BooleanQuery => b.clauses().foldLeft((new BooleanQuery.Builder).add(query, FILTER))(_ add _).build
      case a: MatchAllDocsQuery => query
      case q: Query => (new BooleanQuery.Builder).add(query, FILTER).add(q, MUST).build
    }
  }

  class NopDecorator extends Decorator {

    def decorate(term: Term): Term = term

    def decorate(name: String): String = name

    def decorate(document: Document) = {}

    def decorate(query: Query): Query = query
  }

}
