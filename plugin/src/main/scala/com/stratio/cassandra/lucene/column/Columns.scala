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

import com.google.common.base.MoreObjects

/** A sorted list of CQL3 logic [[Column]]s.
  *
  * @param columns the [[Column]]s composing this
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@scala.annotation.varargs
case class Columns(columns: Column[_]*) extends Traversable[Column[_]] with java.lang.Iterable[Column[_]] {

  /** @constructor create a new columns with a list of columns. */
  def this(columns: Traversable[Column[_]]) = this(columns.toArray: _*)

  /** @constructor create a new empty columns. */
  def this() = this(Array[Column[_]]())

  override def foreach[U](f: Column[_] => U) = columns.foreach(f)

  override def iterator: java.util.Iterator[Column[_]] = {
    import collection.JavaConversions._
    columns.iterator
  }

  /** Returns a copy of this with the specified column appended. */
  def +(column: Column[_]): Columns =
    new Columns(columns :+ column)

  /** Returns a copy of this with the specified columns appended. */
  def +(columns: Columns): Columns =
    new Columns(this.columns ++ columns)

  override def head: Column[_] =
    if (columns.isEmpty) null else columns.head

  /** Returns copy of this with only the columns with the specified full name. */
  def withFieldName(name: String): Columns =
    new Columns(filter(_.fieldName == name))

  /** Returns copy of this with only the columns with the specified cell name. */
  def withCellName(name: String): Columns = {
    lazy val cellName = Column.parse(name).cellName
    new Columns(filter(_.cellName == cellName))
  }

  /** Returns copy of this with only the columns with the specified mapper name. */
  def withMapperName(name: String): Columns = {
    lazy val mapperName = Column.parse(name).mapperName
    new Columns(filter(_.mapperName == mapperName))
  }

  /** Returns copy of this without the columns deleted at the specified time.
    *
    * @param timeInSec the max allowed UNIX time in seconds
    */
  def withoutDeleted(timeInSec: Int): Columns =
    new Columns(filterNot(_.isDeleted(timeInSec)))

  def add[A](column: Column[A]) =
    this + column

  def add[A](columns: Columns) =
    this + columns

  def add[A](cellName: String) =
    this + Column(cellName)

  def add[A](cellName: String, value: A) =
    this + Column(cellName, value = Option(value))

  override def toString: String =
    columns.foldLeft(MoreObjects.toStringHelper(this))((helper, column) =>
      helper.add(column.fieldName, column.value)).toString

}