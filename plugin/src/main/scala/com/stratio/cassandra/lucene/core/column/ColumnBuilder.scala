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
  * Builder for [[Column]].
  *
  * @param cellName     the name of the base cell
  * @param udtNames     the child UDT fields
  * @param mapNames     the child map keys
  * @param deletionTime the deletion time in seconds
  * @author Andres de la Pena {{{<adelapena@stratio.com>}}}
  */
case class ColumnBuilder private(cellName: String,
                                 udtNames: List[String] = Nil,
                                 mapNames: List[String] = Nil,
                                 deletionTime: Option[Integer] = None) {

  def this(cellName: String) =
    this(cellName = cellName, udtNames = Nil, mapNames = Nil, deletionTime = None)

  def this(cellName: String, deletionTime: Int) =
    this(cellName = cellName, udtNames = Nil, mapNames = Nil, deletionTime = Option(deletionTime))

  def withUDTName(name: String): ColumnBuilder =
    copy(udtNames = udtNames :+ name)

  def withMapName(name: String): ColumnBuilder =
    copy(mapNames = mapNames :+ name)

  def build[A](value: A): Column[A] =
    new Column(cellName, udtNames, mapNames, Option(value), deletionTime)

  def build[A](): Column[A] =
    new Column[A](cellName, udtNames, mapNames, None, deletionTime)
}
