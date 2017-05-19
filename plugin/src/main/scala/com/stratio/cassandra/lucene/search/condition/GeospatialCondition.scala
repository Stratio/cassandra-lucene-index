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
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper
import com.stratio.cassandra.lucene.schema.mapping.Mapper
import org.apache.lucene.search.Query
import org.apache.lucene.spatial.SpatialStrategy

/**
 * [[Condition]] for geospatial searches.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost____ the boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}}}.
 * @param field_____ the name of the field to be matched
 * @param queryTypeName the name of the type of query to be shown on user-directed messages
 *
 */
abstract class GeospatialCondition(val boost____ : Float, val field_____ : String, val queryTypeName: String) extends SingleFieldCondition(boost____ , field_____) {

    /** @inheritdoc */
    override def doQuery(schema : Schema) : Query = {

        // Get the spatial strategy from the mapper
        val mapper : Mapper = schema.mapper(field_____)
        if (mapper == null) {
            throw new IndexException(s"No mapper found for field '$field_____'")
        } else {
            val strategy: SpatialStrategy = mapper match {
                case geoShapeMapper: GeoShapeMapper => geoShapeMapper.strategy
                case geoPointMapper: GeoPointMapper => geoPointMapper.strategy
                case _ => throw new IndexException(s"'$queryTypeName' search requires a 'geo_point' or 'geo_shape' mapper but found $field_____ : $mapper");
            }
            // Delegate query creation
            doQuery(strategy)
        }
    }

    /** @inheritdoc */
    def doQuery(strategy: SpatialStrategy ) : Query
}
