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

import com.google.common.base.MoreObjects

/**
  * A sorted list of CQL3 logic [[Column]]s.
  *
  * @param columns the [[Column]]s composing this
  * @author Andres de la Pena { @literal <adelapena@stratio.com>}
  */
case class Columns(columns: Column[_]*) extends Traversable[Column[_]] with java.lang.Iterable[Column[_]] {

  def this(columns: Traversable[Column[_]]) = this(columns.toArray: _*)

  override def foreach[U](f: Column[_] => U) = columns.foreach(f)

  override def iterator: java.util.Iterator[Column[_]] = {
    import collection.JavaConversions._
    columns.iterator
  }

  def +(column: Column[_]): Columns =
    new Columns(columns :+ column)

  def +:(column: Column[_]): Columns =
    new Columns(column +: columns)

  def +(columns: Columns): Columns =
    new Columns(this.columns ++ columns)

  override def head(): Column[_] = if (columns.isEmpty) null else columns.head // TODO: Use option

  /**
    * Returns the columns with the specified full name.
    *
    * @param name a full name
    */
  def getByFullName(name: String): Columns =
    new Columns(filter(_.fullName == name))

  /**
    * Returns the columns with the specified cell name.
    *
    * @param name a cell name
    */
  def getByCellName(name: String): Columns =
    new Columns(filter(_.cellName == Column.parseCellName(name)))

  /**
    * Returns the columns with the specified mapper name.
    *
    * @param name a mapper name
    */
  def getByMapperName(name: String): Columns =
    new Columns(filter(_.mapperName == Column.parseMapperName(name)))

  /**
    * Returns the columns which aren't deletions at the specified time.
    *
    * @param nowInSec the max allowed time in seconds
    * @return this without deleted columns
    */
  def cleanDeleted(nowInSec: Int): Columns =
    new Columns(filterNot(_.isDeleted(nowInSec)))

  def add[A](column: Column[A]) =
    this + column

  def add[A](columns: Columns) =
    this + columns

  def add[A](cellName: String) =
    this + Column.build(cellName)

  def add[A](cellName: String, value: A) =
    this + Column.build(cellName, value)

  def adder(cellName: String): ColumnAdder =
    ColumnAdder(new ColumnBuilder(cellName), this)

  def adder(cellName: String, deletionTime: Int): ColumnAdder =
    ColumnAdder(new ColumnBuilder(cellName, deletionTime), this)

  override def toString: String =
    columns.foldLeft(MoreObjects.toStringHelper(this))((a, c) => a.add(c.fullName, c.value.orNull(null))).toString

}
object Columns {

  @Deprecated
  def build():Columns = Columns()

}