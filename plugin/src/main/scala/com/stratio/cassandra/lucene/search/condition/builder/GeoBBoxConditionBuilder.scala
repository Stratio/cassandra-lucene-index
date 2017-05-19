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
import com.stratio.cassandra.lucene.search.condition.GeoBBoxCondition

/**
 * [[ConditionBuilder]] for building a new [[GeoBBoxCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param field the name of the field to be matched
 * @param minLatitude the minimum accepted latitude
 * @param maxLatitude the maximum accepted latitude
 * @param minLongitude the minimum accepted longitude
 * @param maxLongitude the maximum accepted longitude
 */
class GeoBBoxConditionBuilder @JsonCreator() (  @JsonProperty("field") val field: String,
                                                @JsonProperty("min_latitude") val minLatitude: Double,
                                                @JsonProperty("max_latitude") val maxLatitude: Double,
                                                @JsonProperty("min_longitude") val minLongitude: Double,
                                                @JsonProperty("max_longitude") val maxLongitude: Double) extends ConditionBuilder[GeoBBoxCondition, GeoBBoxConditionBuilder] {



    /** @inheritdoc*/
    override def build: GeoBBoxCondition =
        new GeoBBoxCondition(boost, field, minLatitude, maxLatitude, minLongitude, maxLongitude)
}
