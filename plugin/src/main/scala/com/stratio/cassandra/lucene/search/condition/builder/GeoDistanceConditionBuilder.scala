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
import com.stratio.cassandra.lucene.common.GeoDistance
import com.stratio.cassandra.lucene.search.condition.GeoDistanceCondition

/**
 * [[ConditionBuilder]] for building a new [[GeoDistanceCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 *
 * @param field The name of the field to be matched.
 * @param latitude The latitude of the reference point.
 * @param longitude The longitude of the reference point.

 * @param maxDistance The max allowed distance.
 */
class GeoDistanceConditionBuilder @JsonCreator() (@JsonProperty("field") val field: String,
                                                  @JsonProperty("latitude") val latitude: Double,
                                                  @JsonProperty("longitude") val longitude: Double,
                                                  @JsonProperty("max_distance") val maxDistance: GeoDistance) extends ConditionBuilder[GeoDistanceCondition, GeoDistanceConditionBuilder] {

  var minDistance : GeoDistance = null

  /**
    * @param value the min allowed distance
    * @return
    */
  @JsonProperty("min_distance")
  def minDistance(value: GeoDistance): GeoDistanceConditionBuilder = {
    this.minDistance = value
    this
  }
  /** @inheritdoc*/
  override def build: GeoDistanceCondition =
          new  GeoDistanceCondition(boost, field, latitude, longitude, minDistance, maxDistance)
}
