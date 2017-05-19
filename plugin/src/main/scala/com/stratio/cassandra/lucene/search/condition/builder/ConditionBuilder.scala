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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.common.Builder
import com.stratio.cassandra.lucene.search.condition.Condition

/**
 * [[Builder]] for creating new [[Condition]]s.
 *
  * @tparam T The type of the [[Condition]] to be created.
 * @tparam K  The specific type of the [[ConditionBuilder]].
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = classOf[BooleanConditionBuilder])
@JsonSubTypes(Array(
                new JsonSubTypes.Type(value = classOf[AllConditionBuilder], name = "all"),
                new JsonSubTypes.Type(value = classOf[BitemporalConditionBuilder], name = "bitemporal"),
                new JsonSubTypes.Type(value = classOf[BooleanConditionBuilder], name = "boolean"),
                new JsonSubTypes.Type(value = classOf[ContainsConditionBuilder], name = "contains"),
               new JsonSubTypes.Type(value = classOf[DateRangeConditionBuilder], name = "date_range"),
               new JsonSubTypes.Type(value = classOf[FuzzyConditionBuilder], name = "fuzzy"),
               new JsonSubTypes.Type(value = classOf[GeoDistanceConditionBuilder], name = "geo_distance"),
               new JsonSubTypes.Type(value = classOf[GeoBBoxConditionBuilder], name = "geo_bbox"),
               new JsonSubTypes.Type(value = classOf[GeoShapeConditionBuilder], name = "geo_shape"),
               new JsonSubTypes.Type(value = classOf[LuceneConditionBuilder], name = "lucene"),
               new JsonSubTypes.Type(value = classOf[MatchConditionBuilder], name = "match"),
               new JsonSubTypes.Type(value = classOf[NoneConditionBuilder], name = "none"),
               new JsonSubTypes.Type(value = classOf[PhraseConditionBuilder], name = "phrase"),
               new JsonSubTypes.Type(value = classOf[PrefixConditionBuilder], name = "prefix"),
               new JsonSubTypes.Type(value = classOf[RangeConditionBuilder], name = "range"),
               new JsonSubTypes.Type(value = classOf[RegexpConditionBuilder], name = "regexp"),
               new JsonSubTypes.Type(value = classOf[WildcardConditionBuilder], name = "wildcard")))
abstract class ConditionBuilder[T >: Condition, K >: ConditionBuilder[T,K]] extends Builder[T] {

    var boost : Float = 1.0f
    /**
     * Returns the [[Condition]] represented by this builder.
     *
     * @return a new condition
     */
    def build : T

    @JsonProperty("boost")
    def boost(_boost : Float ) : K = {
      this.boost=_boost
      this.asInstanceOf[K]
    }
}
