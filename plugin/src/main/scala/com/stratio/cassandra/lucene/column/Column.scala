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

/** A cell of a CQL3 logic column, which in most cases is different from a storage engine column.
  *
  * @param cellName the name of the base cell
  * @param udtNames the UDT fields
  * @param mapNames the map keys
  * @param value    the optional value
  * @tparam A the value type
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class Column[A](
    cellName: String,
    udtNames: List[String] = Nil,
    mapNames: List[String] = Nil,
    value: Option[A] = None) {

  if (StringUtils.isBlank(cellName)) throw new IndexException("Cell name shouldn't be blank")

  private[this] lazy val udtSuffix = ("" /: udtNames) (_ + UDT_SEPARATOR + _)
  private[this] lazy val mapSuffix = ("" /: mapNames) (_ + MAP_SEPARATOR + _)

  /** The columns field name, composed by cell name, UDT names and map names. */
  lazy val fieldName: String = cellName + udtSuffix + mapSuffix

  /** The columns mapper name, composed by cell name and UDT names, without map names. */
  lazy val mapperName: String = cellName + udtSuffix

  lazy val mapperNames: List[String] = cellName :: udtNames

  def isEmpty: Boolean = value.isEmpty

  /** Returns a copy of this with the specified name appended to the list of UDT names. */
  def withUDTName(name: String): Column[_] = copy(udtNames = udtNames :+ name)

  /** Returns a copy of this with the specified name appended to the list of map names. */
  def withMapName(name: String): Column[_] = copy(mapNames = mapNames :+ name)

  /** Returns a copy of this with the specified value. */
  def withValue[B](value: B): Column[B] =
    if (value == null && isEmpty || this.value.exists(_ equals value)) {
      this.asInstanceOf[Column[B]]
    } else {
      copy(value = Option(value))
    }

  /** Returns a copy of this with the specified decomposed value. */
  def withValue[B](bb: ByteBuffer, t: AbstractType[_]): Column[_] = withValue(compose(bb, t))

  /** Returns the name for fields. */
  def fieldName(field: String): String = field + mapSuffix

  /** Returns a [[Columns]] composed by this and the specified column. */
  def +(column: Column[_]): Columns = Columns(this, column)

  /** Returns a [[Columns]] composed by this and the specified columns. */
  def +(columns: Columns): Columns = Columns(this) + columns

  /** @inheritdoc */
  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("cell", cellName)
      .add("name", fieldName)
      .add("value", value)
      .toString
}

/** Companion object for [[Column]]. */
object Column {

  private val UDT_SEPARATOR = "."
  private val MAP_SEPARATOR = "$"

  private[this] val UDT_PATTERN = Pattern.quote(UDT_SEPARATOR)
  private[this] val MAP_PATTERN = Pattern.quote(MAP_SEPARATOR)

  def apply(cellName: String): Column[_] = new Column(cellName = cellName)

  def parse(name: String): Column[_] = {
    val x = name.split(MAP_PATTERN)
    val mapNames = x.drop(1).toList
    val y = x.head.split(UDT_PATTERN)
    val cellName = y.head
    val udtNames = y.drop(1).toList
    new Column(cellName, udtNames, mapNames)
  }

  def compose(bb: ByteBuffer, t: AbstractType[_]): Any = t match {
    case sdt: SimpleDateType => new Date(sdt.toTimeInMillis(bb))
    case _ => t.compose(bb)
  }
}
