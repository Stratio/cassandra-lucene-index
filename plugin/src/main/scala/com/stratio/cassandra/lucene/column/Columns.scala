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

import com.google.common.base.MoreObjects.toStringHelper

/** An immutable sorted list of CQL3 logic [[Column]]s.
  *
  * @param columns the [[Column]]s composing this
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@scala.annotation.varargs
case class Columns(private val columns: List[Column]) extends Traversable[Column] {

  /** @constructor create a new empty columns. */
  def this() = this(Nil)

  /** @inheritdoc */
  override def isEmpty: Boolean = columns.isEmpty

  /** @inheritdoc */
  override def foreach[A](f: Column => A): Unit = columns.foreach(f)

  /** Returns a copy of this with the specified column prepended in O(1) time. */
  def ::(column: Column): Columns = new Columns(column :: columns)

  /** Returns a copy of this with the specified column appended in O(n) time. */
  def +(column: Column): Columns = new Columns(columns :+ column)

  /** Returns a copy of this with the specified columns appended. */
  def ++(columns: Columns): Columns = new Columns(this.columns ++ columns)

  /** Returns the value of the first column with the specifed mapper name. */
  def valueForField(field: String): Any = columns.find(_.field == field).flatMap(_.value).orNull

  /** Runs the specified function over each column with the specified field name. */
  def foreachWithMapper[A](field: String)(f: Column => A): Unit = {
    val mapper = Column.parseMapperName(field)
    columns.foreach(column => if (column.mapper == mapper) f(column))
  }

  /** Returns a copy of this with the specified column appended. */
  def add(cell: String): Columns = this + Column(cell)

  /** Returns a copy of this with the specified column appended. */
  def add(cell: String, value: Any): Columns = this + Column(cell, value = Option(value))

  /** @inheritdoc */
  override def toString: String = (toStringHelper(this) /: columns) ((helper, column) =>
    helper.add(column.field, column.value)).toString

}

/** Companion object for [[Columns]]. */
object Columns {

  /** An empty columns. */
  val empty: Columns = new Columns

  /** Returns a new empty columns. */
  def apply: Columns = empty

  /** Returns a new [[Columns]] composed by the specified [[Column]]s. */
  def apply(columns: Traversable[Column]): Columns = new Columns(columns.toList)

  /** Returns a new [[Columns]] composed by the specified [[Column]]s. */
  def apply(columns: Column*): Columns = new Columns(columns.toList)

}
