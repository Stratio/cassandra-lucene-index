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
  * @param value        the optional value
  * @param deletionTime the optional deletion time in seconds
  * @tparam A the value type
  * @author Andres de la Pena {{{<adelapena@stratio.com>}}}
  */
case class Column[A](cellName: String,
                     udtNames: List[String] = Nil,
                     mapNames: List[String] = Nil,
                     value: Option[A] = None,
                     deletionTime: Option[Integer] = None) {

  if (StringUtils.isBlank(cellName)) throw new IndexException("Column name is blank")

  lazy val udtSuffix = udtNames.foldLeft("")((a, n) => a + Column.UDT_SEPARATOR + n)
  lazy val mapSuffix = mapNames.foldLeft("")((a, n) => a + Column.MAP_SEPARATOR + n)
  lazy val fullName = cellName + udtSuffix + mapSuffix
  lazy val mapperName = cellName + udtSuffix

  def withUDTName(name: String): Column[_] =
    copy(udtNames = udtNames :+ name)

  def withMapName(name: String): Column[_] =
    copy(mapNames = mapNames :+ name)

  def withDeletionTime(deletionTime: Int): Column[_] =
    copy(deletionTime = Some(deletionTime))

  def withValue[B](value: B): Column[B] =
    copy(value = Some(value))

  def fieldName(field: String): String =
    field + mapSuffix

  /**
    * Returns if this is a deletion at the specified timestamp.
    * This happens if value is not defined, or if deletionTime is defined and it isn't after the specified time.
    *
    * @param timeInSec an UNIX timestamp in seconds
    * @return true if this is a deletion, false otherwise
    */
  def isDeleted(timeInSec: Int): Boolean =
    value.isEmpty || deletionTime.exists(timeInSec >= _)

  def +(column: Column[_]): Columns =
    Columns(this, column)

  def +(columns: Columns): Columns =
    Columns(this) + columns

  /** @inheritdoc */
  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("cell", cellName)
      .add("name", fullName)
      .add("value", value.getOrElse("null"))
      .add("deletionTime", deletionTime.getOrElse("null"))
      .toString
}

object Column {

  val UDT_SEPARATOR: String = "."
  val MAP_SEPARATOR: String = "$"
  val UDT_PATTERN: String = Pattern.quote(UDT_SEPARATOR)
  val MAP_PATTERN: String = Pattern.quote(MAP_SEPARATOR)
  val NAME_PATTERN: Pattern = Pattern.compile("[^(\\$|\\.)]*[\\.[^(\\$|\\.)]]*[\\$[^(\\$|\\.)]]*")

  def apply(cellName: String): Column[_] =
    new Column(cellName = cellName)

  def isTuple(name: String): Boolean = name contains UDT_SEPARATOR

  def check(name: String) {
    if (!NAME_PATTERN.matcher(name).matches) {
      throw new IndexException("Name {} doesn't satisfy the mandatory pattern {}", name, NAME_PATTERN.pattern)
    }
  }

  def parseMapperName(field: String): String =
    field.split(MAP_PATTERN)(0)

  def parseCellName(field: String): String =
    field.split(UDT_PATTERN)(0).split(MAP_PATTERN)(0)

  def build[A](cellName: String, value: A): Column[A] =
    Column(cellName = cellName, value = Option(value))

  def build[A](cellName: String): Column[A] =
    Column(cellName = cellName)

  def builder(cellName: String, deletionTime: Int): ColumnBuilder =
    new ColumnBuilder(cellName, deletionTime)

  def builder(cellName: String): ColumnBuilder =
    new ColumnBuilder(cellName)
}
