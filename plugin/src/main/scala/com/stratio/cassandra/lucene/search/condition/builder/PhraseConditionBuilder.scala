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
import com.stratio.cassandra.lucene.search.condition.PhraseCondition

/**
 * [[ConditionBuilder]] for building a new [[PhraseCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param field the name of the field to be matched
 * @param value the phrase terms to be matched
 */
class PhraseConditionBuilder @JsonCreator() ( @JsonProperty("field") val field: String,
                                              @JsonProperty("value") val value: String) extends ConditionBuilder[PhraseCondition, PhraseConditionBuilder] {


  private var slop: Int = PhraseCondition.DEFAULT_SLOP

  /**
    * @param _slop the number of other words permitted between words in phrase to set
    * @return
    */
  @JsonProperty("slop")
  def slop(_slop: Int) : PhraseConditionBuilder = {
    slop = _slop
    this
  }
  /** @inheritdoc*/
  override def build: PhraseCondition =
    new PhraseCondition(boost, field, value, slop)
}
