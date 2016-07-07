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
package com.stratio.cassandra.lucene.core.column

import java.util.regex.Pattern

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import org.apache.commons.lang3.StringUtils

/**
  * A cell of a CQL3 logic column, which in most cases is different from a storage engine column.
  *
  * @param cellName     the name of the base cell
  * @param udtNames     the UDT fields
  * @param mapNames     the map keys
  * @param deletionTime the deletion time in seconds
  * @param value        the optional value
  * @tparam A the value type
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class Column[A](cellName: String,
                     udtNames: List[String] = Nil,
                     mapNames: List[String] = Nil,
                     deletionTime: Int = Column.NO_DELETION_TIME,
                     value: Option[A] = None) {

  if (StringUtils.isBlank(cellName)) throw new IndexException("Cell name shouldn't be blank")

  private[this] lazy val udtSuffix = udtNames.foldLeft("")((a, n) => a + Column.UDT_SEPARATOR + n)
  private[this] lazy val mapSuffix = mapNames.foldLeft("")((a, n) => a + Column.MAP_SEPARATOR + n)

  /** The columns full name, composed by cell name, UDT names and map names. */
  lazy val fullName = cellName + udtSuffix + mapSuffix

  /** The columns mapper name, composed by cell name and UDT names, without map names. */
  lazy val mapperName = cellName + udtSuffix

  /** Returns a copy of this with the specified name appended to the list of UDT names. */
  def withUDTName(name: String): Column[_] =
    copy(udtNames = udtNames :+ name)

  /** Returns a copy of this with the specified name appended to the list of map names. */
  def withMapName(name: String): Column[_] =
    copy(mapNames = mapNames :+ name)

  /** Returns a copy of this with the specified deletion UNIX time in seconds. */
  def withDeletionTime(deletionTime: Int): Column[_] =
    copy(deletionTime = deletionTime)

  /** Returns a copy of this with the specified value. */
  def withValue[B](value: B): Column[B] =
    copy(value = Option(value))

  /** Returns the name for fields. */
  def fieldName(field: String): String =
    field + mapSuffix

  /** Returns if this is a deletion at the specified timestamp.
    *
    * This happens if value is not defined, or if deletionTime isn't after the specified time.
    *
    * @param timeInSec an UNIX timestamp in seconds
    * @return true if this is a deletion, false otherwise
    */
  def isDeleted(timeInSec: Int): Boolean =
    value.isEmpty || deletionTime <= timeInSec

  /** Returns a [[Columns]] composed by this and the specified column. */
  def +(column: Column[_]): Columns =
    Columns(this, column)

  /** Returns a [[Columns]] composed by this and the specified columns. */
  def +(columns: Columns): Columns =
    Columns(this) + columns

  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("cell", cellName)
      .add("name", fullName)
      .add("value", value.getOrElse("null"))
      .add("deletionTime", deletionTime)
      .toString
}

object Column {

  val NO_DELETION_TIME: Int = Int.MaxValue

  private val UDT_SEPARATOR: String = "."
  private val MAP_SEPARATOR: String = "$"

  private[this]  val UDT_PATTERN: String = Pattern.quote(UDT_SEPARATOR)
  private[this] val MAP_PATTERN: String = Pattern.quote(MAP_SEPARATOR)

  def apply(cellName: String): Column[_] =
    new Column(cellName = cellName)

  /** Returns true if the specified column full name corresponds to a tuple. */
  def isMultiColumn(fullName: String): Boolean = fullName contains UDT_SEPARATOR

  /** Returns the mapper name contained in the specified name. */
  def parseMapperName(name: String): String =
    name.split(MAP_PATTERN)(0)

  /** Returns the cell name contained in the specified name. */
  def parseCellName(name: String): String =
    name.split(UDT_PATTERN)(0).split(MAP_PATTERN)(0)

  /** Returns the columns names contained in the specified name. */
  def parseColumnNames(name: String): Array[String] =
    name.split(UDT_PATTERN)
}
