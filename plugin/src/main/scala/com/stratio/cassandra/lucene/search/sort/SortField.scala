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
package com.stratio.cassandra.lucene.search.sort

import java.util

import com.stratio.cassandra.lucene.schema.Schema

/**
  * A sorting for a field of a search.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  * @param _reverse [[Boolean.true]] if natural order should be reversed.
  */
abstract class SortField(val _reverse:  Boolean) {

/**
  * Returns the Lucene's [[org.apache.lucene.search.SortField]] representing this [[SortField]].
  *
  * @param schema the [[Schema]] to be used
  * @return the Lucene's sort field
  */
def sortField (schema: Schema): org.apache.lucene.search.SortField

/**
  * Returns the names of the involved fields.
  *
  * @return the names of the involved fields
  */
def postProcessingFields: util.Set[String]

def toString: String

def equals (any: Any): Boolean

def hashCode: Int
}

object SortField {
  /** The default reverse option. */
  def DEFAULT_REVERSE: Boolean = false
}