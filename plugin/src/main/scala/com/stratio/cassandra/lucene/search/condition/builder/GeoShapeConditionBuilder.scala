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
import com.stratio.cassandra.lucene.common.{GeoOperation, GeoShapes, JTSNotFoundException}
import com.stratio.cassandra.lucene.search.condition.GeoShapeCondition

/**
 * [[ConditionBuilder]] for building a new [[GeoShapeCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param field the name of the field
 * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
 */
class GeoShapeConditionBuilder @JsonCreator() ( @JsonProperty("field") val field: String,
                                                @JsonProperty("shape") val shape: GeoShapes.GeoShape) extends ConditionBuilder[GeoShapeCondition, GeoShapeConditionBuilder] {

    //* @param operation the name of the spatial operation
    @JsonProperty("operation")
    var operation: GeoOperation = null

    /** @inheritdoc*/
    override def build: GeoShapeCondition = {
        try {
            new GeoShapeCondition(boost, field, shape, operation)
        } catch {
            case e:NoClassDefFoundError => throw new JTSNotFoundException()
        }
    }
}
