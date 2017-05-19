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
package com.stratio.cassandra.lucene.search.sort.builder

import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.common.Builder
import com.stratio.cassandra.lucene.search.sort.SortField

/**
 * [[Builder]] for building a new [[SortField]].
 *
 * @tparam T the [[SortField]]
 * @tparam K the [[SortField]] builder
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = classOf[SimpleSortFieldBuilder])
@JsonSubTypes(Array(
                new JsonSubTypes.Type(value = classOf[SimpleSortFieldBuilder], name = "simple"),
                new JsonSubTypes.Type(value = classOf[GeoDistanceSortFieldBuilder], name = "geo_distance")
                ))
abstract class SortFieldBuilder[T <: SortField, K <: SortFieldBuilder[_,_]] extends Builder[T] {

  var reverse : Boolean = false

  @JsonProperty("reverse")
  def reverse(value: Boolean) : SortFieldBuilder[T,K] = {
    this.reverse=value
    this
  }

}