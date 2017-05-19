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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.search.condition.BitemporalCondition

/**
 * [[ConditionBuilder]] for building a new [[BitemporalCondition]].
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 * @param field the name of the field to be matched




 */
class BitemporalConditionBuilder @JsonCreator()(@JsonProperty("field") val field: String) extends ConditionBuilder[BitemporalCondition, BitemporalConditionBuilder] {

  private var vtFrom: Any = null
  private var vtTo: Any= null
  private var ttFrom: Any= null
  private var ttTo: Any= null


  /**
    * @param value the valid time start to be set
    * @return
    */
  @JsonProperty("vt_from")
  def vtFrom(value: Any): BitemporalConditionBuilder = {
    this.vtFrom=value
    this
  }

  /**
    * @param value the valid time end to be set
    * @return
    */
  @JsonProperty("vt_to")
  def vtTo(value:Any ): BitemporalConditionBuilder = {
    this.vtTo=value
    this

  }

  /**
    * @param value the transaction time start to be set
    * @return
    */
  @JsonProperty("tt_from")
  def ttFrom(value:Any ) : BitemporalConditionBuilder = {
    this.ttFrom=value
    this
  }

  /**
    * @param value the transaction time end to be set
    * @return
    */
  @JsonProperty("tt_to")
  def ttTo(value:Any ) : BitemporalConditionBuilder = {
    this.ttTo=value
    this
  }


    /** @inheritdoc */
    override def build: BitemporalCondition =
       new BitemporalCondition(boost, field, vtFrom, vtTo, ttFrom, ttTo)
}
