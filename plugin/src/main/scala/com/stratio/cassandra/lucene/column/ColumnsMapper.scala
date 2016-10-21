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
package com.stratio.cassandra.lucene.column

import java.math.{BigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.stratio.cassandra.lucene.IndexException
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows.{Cell, ComplexColumnData, Row}
import org.apache.cassandra.serializers.CollectionSerializer
import org.apache.cassandra.transport.Server._
import org.apache.cassandra.utils.ByteBufferUtil

import scala.annotation.tailrec
import scala.collection.JavaConversions._

/** Maps Cassandra rows to [[Columns]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
object ColumnsMapper {

  /** Returns a [[Columns]] representing the specified row.
    *
    * @param row the Cassandra row to be mapped
    */
  def columns(row: Row): Columns = {
    row.columns().foldLeft(Columns())(
      (cs, columnDefinition) =>
        if (columnDefinition.isComplex)
          cs + columns(row.getComplexColumnData(columnDefinition))
        else
          cs + columns(row.getCell(columnDefinition))
    )
  }

  private[column] def columns(complexColumnData: ComplexColumnData): Columns = {
    complexColumnData.foldLeft(Columns())((cs, cell) => cs + columns(cell))
  }

  private[column] def columns(cell: Cell): Columns = {
    if (cell == null) return Columns()
    val isTombstone = cell.isTombstone
    val name = cell.column.name.toString
    val comparator = cell.column.`type`
    val value = cell.value
    val column = new Column(cellName = name, deletionTime = cell.localDeletionTime)
    comparator match {
      case setType: SetType[_] if !setType.isFrozenCollection =>
        val itemComparator = setType.nameComparator
        val itemValue = cell.path.get(0)
        columns(isTombstone, column, itemComparator, itemValue)
      case listType: ListType[_] if !listType.isFrozenCollection =>
        val itemComparator = listType.valueComparator
        columns(isTombstone, column, itemComparator, value)
      case mapType: MapType[_, _] if !mapType.isFrozenCollection =>
        val itemComparator = mapType.valueComparator
        val keyValue = cell.path.get(0)
        val keyComparator = mapType.nameComparator
        val nameSuffix = keyComparator.compose(keyValue).toString
        columns(isTombstone, column.withMapName(nameSuffix), itemComparator, value)
      case _ =>
        columns(isTombstone, column, comparator, value)
    }
  }

  private[column] def columns(
      isTombstone: Boolean,
      column: Column[_],
      abstractType: AbstractType[_],
      value: ByteBuffer): Columns = abstractType match {
    case setType: SetType[_] =>
      columns(isTombstone, column, setType, value)
    case listType: ListType[_] =>
      columns(isTombstone, column, listType, value)
    case mapType: MapType[_, _] =>
      columns(isTombstone, column, mapType, value)
    case userType: UserType =>
      columns(isTombstone, column, userType, value)
    case tupleType: TupleType =>
      columns(isTombstone, column, tupleType, value)
    case _ =>
      Columns(column.withValue(compose(value, abstractType)))
  }

  private[this] def columns(
      isTombstone: Boolean,
      column: Column[_],
      set: SetType[_],
      value: ByteBuffer): Columns = {
    if (isTombstone) return Columns(column)
    val nameType = set.nameComparator
    val bb = ByteBufferUtil.clone(value) // CollectionSerializer read functions are impure
    (0 until frozenCollectionSize(bb)).foldLeft(Columns())(
      (cs, n) => {
        val itemValue = frozenCollectionValue(bb)
        cs + columns(isTombstone, column, nameType, itemValue)
      })
  }

  private[this] def columns(
      isTombstone: Boolean,
      column: Column[_],
      list: ListType[_],
      value: ByteBuffer): Columns = {
    if (isTombstone) return Columns(column)
    val valueType = list.valueComparator
    val bb = ByteBufferUtil.clone(value) // CollectionSerializer read functions are impure
    (0 until frozenCollectionSize(bb)).foldLeft(Columns())(
      (cs, n) => {
        val itemValue = frozenCollectionValue(bb)
        cs + columns(isTombstone, column, valueType, itemValue)
      })
  }

  private[this] def columns(
      isTombstone: Boolean,
      column: Column[_],
      map: MapType[_, _],
      value: ByteBuffer): Columns = {
    if (isTombstone) return Columns(column)
    val itemKeysType = map.nameComparator
    val itemValuesType = map.valueComparator
    val bb = ByteBufferUtil.clone(value) // CollectionSerializer read functions are impure
    (0 until frozenCollectionSize(bb)).foldLeft(Columns())(
      (cs, n) => {
        val itemKey = frozenCollectionValue(bb)
        val itemValue = frozenCollectionValue(bb)
        val itemName = itemKeysType.compose(itemKey).toString
        cs + columns(isTombstone, column.withMapName(itemName), itemValuesType, itemValue)
      })
  }

  private[this] def columns(
      isTombstone: Boolean,
      column: Column[_],
      udt: UserType,
      value: ByteBuffer): Columns = {
    if (isTombstone) return Columns(column)
    val itemValues = udt.split(value)
    (0 until udt.fieldNames.size).foldLeft(Columns())(
      (cs, i) => {
        val itemName = udt.fieldNameAsString(i)
        val itemType = udt.fieldType(i)
        val itemValue = itemValues(i)
        if (isTombstone || itemValue == null) {
          cs + column.withUDTName(itemName)
        } else {
          cs + columns(isTombstone, column.withUDTName(itemName), itemType, itemValue)
        }
      })
  }

  private[this] def columns(
      isTombstone: Boolean,
      column: Column[_],
      tuple: TupleType,
      value: ByteBuffer): Columns = {
    if (isTombstone) return Columns(column)
    val itemValues = tuple.split(value)
    (0 until tuple.size).foldLeft(Columns())(
      (cs, i) => {
        val itemName = i.toString
        val itemType = tuple.`type`(i)
        val itemValue = itemValues(i)
        if (isTombstone || itemValue == null) {
          cs + column.withUDTName(itemName)
        } else {
          cs + columns(isTombstone, column.withUDTName(itemName), itemType, itemValue)
        }
      })
  }

  private[this] def frozenCollectionSize(bb: ByteBuffer): Int =
    CollectionSerializer.readCollectionSize(bb, CURRENT_VERSION)

  private[this] def frozenCollectionValue(bb: ByteBuffer): ByteBuffer =
    CollectionSerializer.readValue(bb, CURRENT_VERSION)

  def compose(bb: ByteBuffer, t: AbstractType[_]): Any = t match {
    case sdt: SimpleDateType => new Date(sdt.toTimeInMillis(bb))
    case _ => t.compose(bb)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Validation
  ///////////////////////////////////////////////////////////////////////////

  def validate(
      metadata: CFMetaData,
      column: String,
      field: String,
      supportedTypes: java.util.List[Class[_]]) {

    val cellName = Column.parse(column).cellName
    val cellDefinition = metadata.getColumnDefinition(UTF8Type.instance.decompose(cellName))

    if (cellDefinition == null) {
      throw new IndexException(s"No column definition '$cellName' for mapper '$field'")
    }
    if (cellDefinition.isStatic) {
      throw new IndexException(s"Lucene indexes are not allowed on static columns as '$column'")
    }

    def checkSupported(t: AbstractType[_], mapper: String) {
      if (!supports(t, supportedTypes)) throw new IndexException(
        s"Type '$t' in column '$mapper' is not supported by mapper '$field'")
    }

    val cellType = cellDefinition.`type`
    val udtNames = Column.parse(column).udtNames
    if (udtNames.isEmpty) {
      checkSupported(cellType, cellName)
    } else {
      var col = Column.apply(cellName)
      var currentType = cellType
      for (i <- udtNames.indices) {
        col = col.withUDTName(udtNames(i))
        val mapperName = col.mapperName
        ColumnsMapper.childType(currentType, udtNames(i)) match {
          case None =>
            throw new IndexException(s"No column definition '$mapperName' for mapper '$field'")
          case Some(n) if i == udtNames.indices.last => checkSupported(n, mapperName)
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
