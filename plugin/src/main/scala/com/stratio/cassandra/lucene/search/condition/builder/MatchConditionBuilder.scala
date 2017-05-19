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
import com.stratio.cassandra.lucene.search.condition.MatchCondition

/**
 * [[ConditionBuilder]] for building a new [[MatchCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param field the name of the field to be matched
 * @param value the value of the field to be matched
 */
class MatchConditionBuilder @JsonCreator() (@JsonProperty("field") val field: String,
                                            @JsonProperty("value") val value: Any) extends ConditionBuilder[MatchCondition, MatchConditionBuilder] {
  //docValues if the generated query should use doc values
  private var docValues: Boolean = MatchCondition.DEFAULT_DOC_VALUES

  @JsonProperty("doc_values")
  def docValues(value:Boolean ): MatchConditionBuilder = {
    this.docValues=value
    this
  }

  /** @inheritdoc*/
  override def build: MatchCondition =
            new MatchCondition(boost, field, value, docValues)
}
