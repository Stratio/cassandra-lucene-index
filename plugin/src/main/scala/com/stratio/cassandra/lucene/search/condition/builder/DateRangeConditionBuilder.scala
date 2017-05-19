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
import com.stratio.cassandra.lucene.common.GeoOperation
import com.stratio.cassandra.lucene.search.condition.{ContainsCondition, DateRangeCondition}

/**
 * [[ConditionBuilder]] for building a new [[DateRangeCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param field the name of the field to be matched
 */
class DateRangeConditionBuilder @JsonCreator() ( @JsonProperty("field") val field: String) extends ConditionBuilder[DateRangeCondition, DateRangeConditionBuilder] {

  // from the lower accepted date, or {{{null} if there is no lower limit
  private var from: Any= null

  // to the upper accepted date, or {{{null} if there is no upper limit
  private var to: Any= null

  // operation the operation
  private var operation: GeoOperation= null


  @JsonProperty("from")
  def from(_from:Any): DateRangeConditionBuilder = {
    this.from = _from
    this
  }

  @JsonProperty("to")
  def to(_to:Any): DateRangeConditionBuilder = {
    this.to = _to
    this
  }

  @JsonProperty("operation")
  def operation(_operation : GeoOperation): DateRangeConditionBuilder = {
    this.operation=_operation
    this
  }
    /** @inheritdoc */
    override def build:DateRangeCondition =
        new DateRangeCondition(boost, field, from, to, operation)
}
