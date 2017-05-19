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
package com.stratio.cassandra.lucene.common

import com.fasterxml.jackson.annotation.JsonCreator
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.util.Logging
import org.apache.lucene.spatial.query.SpatialOperation

/**
 * Enum representing a spatial operation.
 */
class GeoOperation(val name: String , val spatialOperation : SpatialOperation) {

    /** @inheritdoc */
    override def toString : String = name.toUpperCase
}

object GeoOperation extends Logging {
    val DEFAULT_GEO_OPERATION = INTERSECTS

    val INTERSECTS : GeoOperation = new GeoOperation("intersects", SpatialOperation.Intersects)
    val IS_WITHIN : GeoOperation = new GeoOperation("is_within", SpatialOperation.IsWithin)
    val CONTAINS : GeoOperation = new GeoOperation("contains", SpatialOperation.Contains)

    def values : List[GeoOperation] = List(INTERSECTS, IS_WITHIN, CONTAINS)

    @JsonCreator
    def parse(value: String) : GeoOperation = {
        var returnObject : Option[GeoOperation]= None
        for (geoOperation <- GeoOperation.values) {
            if (geoOperation.name.equals(value)) {
                returnObject = Some(geoOperation)
            }
        }
        returnObject.getOrElse(throw new IndexException("Invalid geographic operation {}", value))
    }
}
