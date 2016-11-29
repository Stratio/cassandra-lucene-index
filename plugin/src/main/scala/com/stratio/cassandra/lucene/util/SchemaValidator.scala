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
package com.stratio.cassandra.lucene.util

import java.math.{BigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.column.Column
import com.stratio.cassandra.lucene.schema.Schema
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.marshal._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/** Object for validating a [[Schema]] against a [[CFMetaData]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
object SchemaValidator {

  /** Validates the specified [[Schema]] against the specified [[CFMetaData]].
    *
    * @param schema a schema
    * @param metadata a table metadata
    */
  def validate(schema: Schema, metadata: CFMetaData): Unit = {
    for (mapper <- schema.mappers.values.asScala; column <- mapper.mappedColumns.asScala) {
      validate(metadata, column, mapper.field, mapper.supportedTypes.asScala.toList)
    }
  }

  def validate(
      metadata: CFMetaData,
      column: String,
      field: String,
      supportedTypes: List[Class[_]]) {

    val cellName = Column.parse(column).cellName
    val cellDefinition = metadata.getColumnDefinition(UTF8Type.instance.decompose(cellName))

    if (cellDefinition == null) {
      throw new IndexException("No column definition '{}' for mapper '{}'", cellName, field)
    }
    if (cellDefinition.isStatic) {
      throw new IndexException("Lucene indexes are not allowed on static columns as '{}'", column)
    }

    def checkSupported(t: AbstractType[_], mapper: String) {
      if (!supports(t, supportedTypes)) {
        throw new IndexException(
          "Type '{}' in column '{}' is not supported by mapper '{}'",
          t,
          mapper,
          field)
      }
    }

    val cellType = cellDefinition.`type`
    val udtNames = Column.parse(column).udtNames
    if (udtNames.isEmpty) {
      checkSupported(cellType, cellName)
    } else {
      var column = Column.apply(cellName)
      var currentType = cellType
      for (i <- udtNames.indices) {
        column = column.withUDTName(udtNames(i))
        childType(currentType, udtNames(i)) match {
          case None => throw new IndexException(
            s"No column definition '${column.mapperName}' for field '$field'")
          case Some(n) if i == udtNames.indices.last => checkSupported(n, column.mapperName)
          case Some(n) => currentType = n
        }
      }
    }
  }

  @tailrec
  def childType(parent: AbstractType[_], child: String): Option[AbstractType[_]] = parent match {
    case t: ReversedType[_] => childType(t.baseType, child)
    case t: SetType[_] => childType(t.nameComparator, child)
    case t: ListType[_] => childType(t.valueComparator, child)
    case t: MapType[_, _] => childType(t.valueComparator, child)
    case t: UserType =>
      (0 until t.fieldNames.size).find(t.fieldNameAsString(_) == child).map(t.fieldType)
    case t: TupleType => (0 until t.size).find(_.toString == child).map(t.`type`)
    case _ => None
  }

  @tailrec
  def supports(
      candidateType: AbstractType[_],
      supportedTypes: Seq[Class[_]]): Boolean = candidateType match {
    case t: ReversedType[_] => supports(t.baseType, supportedTypes)
    case t: SetType[_] => supports(t.getElementsType, supportedTypes)
    case t: ListType[_] => supports(t.getElementsType, supportedTypes)
    case t: MapType[_, _] => supports(t.getValuesType, supportedTypes)
    case _ =>
      val native = nativeType(candidateType)
      supportedTypes.exists(_ isAssignableFrom native)
  }

  def nativeType(validator: AbstractType[_]): Class[_] = validator match {
    case _: UTF8Type | _: AsciiType => classOf[String]
    case _: SimpleDateType | _: TimestampType => classOf[Date]
    case _: UUIDType | _: LexicalUUIDType | _: TimeUUIDType => classOf[UUID]
    case _: ShortType => classOf[java.lang.Short]
    case _: ByteType => classOf[java.lang.Byte]
    case _: Int32Type => classOf[Integer]
    case _: LongType => classOf[java.lang.Long]
    case _: IntegerType => classOf[BigInteger]
    case _: FloatType => classOf[java.lang.Float]
    case _: DoubleType => classOf[java.lang.Double]
    case _: DecimalType => classOf[BigDecimal]
    case _: BooleanType => classOf[java.lang.Boolean]
    case _: BytesType => classOf[ByteBuffer]
    case _: InetAddressType => classOf[InetAddress]
    case _ => throw new IndexException(s"Unsupported Cassandra data type: ${validator.getClass}")
  }

}
