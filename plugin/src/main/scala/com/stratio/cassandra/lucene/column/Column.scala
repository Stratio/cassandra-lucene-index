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

import java.nio.ByteBuffer
import java.util.Date
import java.util.regex.Pattern

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.column.Column._
import org.apache.cassandra.db.marshal.{AbstractType, SimpleDateType}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.EMPTY

/** A cell of a CQL3 logic column, which in most cases is different from a storage engine column.
  *
  * @param cell the name of the base cell
  * @param udt the UDT suffix
  * @param map the map suffix
  * @param value    the optional value
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class Column(cell: String,
    udt: String = EMPTY,
    map: String = EMPTY,
    value: Option[_] = None) {

  if (StringUtils.isBlank(cell)) throw new IndexException("Cell name shouldn't be blank")

  /** The columns mapper name, composed by cell name and UDT names, without map names. */
  lazy val mapper: String = cell.concat(udt)

  /** The columns field name, composed by cell name, UDT names and map names. */
  lazy val field: String = mapper.concat(map)

  /** Returns `true` if the value is not defined, `false` otherwise. */
  def isEmpty: Boolean = value.isEmpty

  /** Returns the value, or null if it is not defined. */
  def valueOrNull: Any = value.orNull

  /** Returns a copy of this with the specified name appended to the list of UDT names. */
  def withUDTName(name: String): Column = copy(udt = udt + UDT_SEPARATOR + name)

  /** Returns a copy of this with the specified name appended to the list of map names. */
  def withMapName(name: String): Column = copy(map = map + MAP_SEPARATOR + name)

  /** Returns a copy of this with the specified value. */
  def withValue[B](value: B): Column = copy(value = Option(value))

  /** Returns a copy of this with the specified decomposed value. */
  def withValue(bb: ByteBuffer, t: AbstractType[_]): Column = withValue(compose(bb, t))

  /** Returns the name for fields. */
  def fieldName(field: String): String = field.concat(map)

  /** Returns a [[Columns]] composed by this and the specified column. */
  def +(column: Column): Columns = Columns(this, column)

  /** Returns a [[Columns]] composed by this and the specified columns. */
  def +(columns: Columns): Columns = this :: columns

  /** @inheritdoc */
  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("cell", cell)
      .add("field", field)
      .add("value", value)
      .toString
}

/** Companion object for [[Column]]. */
object Column {

  private val UDT_SEPARATOR = "."
  private val MAP_SEPARATOR = "$"

  private[this] val UDT_PATTERN = Pattern.quote(UDT_SEPARATOR)

  def apply(cell: String): Column = new Column(cell = cell)

  def parseCellName(name: String): String = {
    val udtSuffixStart = name.indexOf(UDT_SEPARATOR)
    if (udtSuffixStart < 0) {
      val mapSuffixStart = name.indexOf(MAP_SEPARATOR)
      if (mapSuffixStart < 0) name else name.substring(0, mapSuffixStart)
    } else name.substring(0, udtSuffixStart)
  }

  def parseMapperName(name: String): String = {
    val mapSuffixStart = name.indexOf(MAP_SEPARATOR)
    if (mapSuffixStart < 0) name else name.substring(0, mapSuffixStart)
  }

  def parseUdtNames(name: String): List[String] = {
    val udtSuffixStart = name.indexOf(UDT_SEPARATOR)
    if (udtSuffixStart < 0) Nil else {
      val mapSuffixStart = name.indexOf(MAP_SEPARATOR)
      val udtSuffix = if (mapSuffixStart < 0) {
        name.substring(udtSuffixStart + 1)
      } else {
        name.substring(udtSuffixStart + 1, mapSuffixStart)
      }
      udtSuffix.split(UDT_PATTERN).toList
    }
  }

  def compose(bb: ByteBuffer, t: AbstractType[_]): Any = t match {
    case sdt: SimpleDateType => new Date(sdt.toTimeInMillis(bb))
    case _ => t.compose(bb)
  }
}
