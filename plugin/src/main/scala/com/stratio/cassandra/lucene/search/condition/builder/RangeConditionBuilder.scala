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
package com.stratio.cassandra.lucene.search.condition.builder

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.search.condition.RangeCondition

/**
 * [[ConditionBuilder]] for building a new [[RangeCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
  * @param field the name of the field to be matched
 */
class RangeConditionBuilder @JsonCreator() (@JsonProperty("field") val field: String) extends ConditionBuilder[RangeCondition, RangeConditionBuilder] {

  // lower the lower value to be matched, or {{{null} if there is no lower limit
  private var lower: Any = null

  // upper the lower value to be matched, or {{{null} if there is no upper limit* @param lower the lower value to be matched, or {{{null} if there is no lower limit
  private var upper: Any= null

  // includeLower {{{true} the lower value must be included, {{{false} otherwise
  private var includeLower: Boolean = RangeCondition.DEFAULT_INCLUDE_LOWER

  // includeUpper if the upper value must be included
  private var includeUpper: Boolean = RangeCondition.DEFAULT_INCLUDE_UPPER

  // docValues if the generated query should use doc values
  private var docValues: Boolean = RangeCondition.DEFAULT_DOC_VALUES



  @JsonProperty("lower")
  def lower(_lower: Any) : RangeConditionBuilder = {
    this.lower=_lower
    this
  }

  // upper the lower value to be matched, or {{{null} if there is no upper limit* @param lower the lower value to be matched, or {{{null} if there is no lower limit
  @JsonProperty("upper")
  def upper(_upper: Any): RangeConditionBuilder = {
    this.upper = _upper
    this
  }

  // includeLower {{{true} the lower value must be included, {{{false} otherwise
  @JsonProperty("include_lower")
  def includeLower(_includeLower: Boolean): RangeConditionBuilder = {
    this.includeLower=_includeLower
    this
  }

  // includeUpper if the upper value must be included
  @JsonProperty("include_upper")
  def includeUpper(_includeUpper: Boolean): RangeConditionBuilder = {
    this.includeUpper=_includeUpper
    this
  }

  // docValues if the generated query should use doc values
  @JsonProperty("doc_values")
  def docValues(_docValues: Boolean): RangeConditionBuilder = {
    this.docValues= _docValues
    this
  }

  /** @inheritdoc*/
  override def build: RangeCondition =
      new RangeCondition(boost, field, lower, upper, includeLower, includeUpper, docValues)
}
