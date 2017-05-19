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

import java.util.stream.Collectors.toList

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.search.condition.{BooleanCondition, Condition}

/**
 * [[ConditionBuilder]] for building a new [[BooleanCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *


 */
class BooleanConditionBuilder @JsonCreator()() extends ConditionBuilder[BooleanCondition, BooleanConditionBuilder] {

  @JsonProperty("must")
  var must : Array[ConditionBuilder[_,_]]=  Array()
  @JsonProperty("should")
  var should : Array[ConditionBuilder[_,_]] = Array()
  @JsonProperty("not")
  var not : Array[ConditionBuilder[_,_]]=  Array()



  /**
    * @param _must the conditions to be added
    * @return
    */
  def must(_must: Array[ConditionBuilder[_,_]]) : BooleanConditionBuilder = {
    this.must ++ _must
    this
  }

  def must(_must: ConditionBuilder[_,_]) : BooleanConditionBuilder = {
    this.must :+ _must
    this
  }

  /**
    * @param _should the conditions to be added
    * @return
    */
  def should(_should: Array[ ConditionBuilder[_,_]]) : BooleanConditionBuilder = {
    this.should ++ _should
    this
  }

  def should(_should: ConditionBuilder[_,_]) : BooleanConditionBuilder = {
    this.should :+ _should
    this
  }

  /**
    * @param _not the conditions to be added
    * @return
    */
  def not(_not: Array[ConditionBuilder[_,_]]) : BooleanConditionBuilder = {
    this.not ++ _not
    this
  }

  def not(_not: ConditionBuilder[_,_]) : BooleanConditionBuilder = {
    this.not :+ _not
    this
  }


  /** @inheritdoc */
  override def build : BooleanCondition =
    new BooleanCondition(boost, must.map(_.build.asInstanceOf[Condition]),
                                should.map(_.build.asInstanceOf[Condition]),
                                not.map(_.build.asInstanceOf[Condition]))

}
