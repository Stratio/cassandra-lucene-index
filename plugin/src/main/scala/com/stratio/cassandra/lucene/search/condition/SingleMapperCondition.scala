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
package com.stratio.cassandra.lucene.search.condition

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.schema.mapping.Mapper
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.Query

/**
  * An abstract [[Condition]] using a specific [[Mapper]].
  *
  * Known subclasses are: <ul> <li> [[BitemporalCondition]] <li> [[DateRangeCondition]] <li> [[GeoDistanceCondition]] <li> [[GeoBBoxCondition]] </ul>
  *
  * @tparam T The specific [[Mapper]] type.
  * @param boost___ the boost for this query clause. Documents matching this clause will (in addition to the normal
  *              weightings) have their score multiplied by { @code boost}.
  * @param field___ the name of the field to be matched
  * @param type  the type of the [[Mapper]]
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class SingleMapperCondition[T <: Mapper](val boost___ : Float,
                                                  val field___ : String,
                                                  val `type`: Class[T]) extends SingleFieldCondition(boost___, field___) {

  /** @inheritdoc*/
  override def doQuery(schema: Schema) : Query = {
    val mapper : Mapper = schema.mapper(field___)
    if (mapper == null) {
      throw new IndexException(s"No mapper found for field '$field___'")
    } else if ( !`type`.isAssignableFrom(mapper.getClass)) {
      throw new IndexException(s"Field '$field___' requires a mapper of type '${`type`}' but found '$mapper'")
    } else {
      doQuery(mapper.asInstanceOf[T], schema.analyzer)
    }
  }

  /**
    * Returns the Lucene [[Query]] representation of this condition.
    *
    * @param mapper The [[Mapper]] to be used.
    * @param analyzer The [[Schema]] [[Analyzer]].
    * @return The Lucene [[Query]] representation of this condition.
    */
  def doQuery(mapper: T , analyzer: Analyzer ) : Query
}