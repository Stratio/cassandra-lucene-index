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

/**
  * Adds a [[Column]] to a [[Columns]].
  *
  * @param builder a [[Column]] builder
  * @param columns the [[Columns]] to be modified
  * @author Andres de la Pena {{{<adelapena@stratio.com>}}}
  */
case class ColumnAdder (builder: ColumnBuilder, columns: Columns) {

  def withUDTName(name: String): ColumnAdder =
    ColumnAdder(builder.withUDTName(name), columns)

  def withMapName(name: String): ColumnAdder =
    ColumnAdder(builder.withMapName(name), columns)

  def add[A](value: A): Columns =
    columns + builder.build(value)

  def add[A](): Columns =
    columns + builder.build()
}
