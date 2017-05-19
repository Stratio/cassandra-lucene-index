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
package com.stratio.cassandra.lucene.search.sort

import java.util
import java.util.Collections

import com.google.common.base.MoreObjects
import com.spatial4j.core.distance.DistanceUtils
import com.spatial4j.core.shape.Point
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.common.GeospatialUtils
import com.stratio.cassandra.lucene.common.GeospatialUtils.CONTEXT
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.queries.function.ValueSource
import org.apache.lucene.spatial.SpatialStrategy

/**
 * [[SortField]] to sort geo points by their distance to a fixed reference point.
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 * @param field the name of the geo point field mapper to use to calculate distance
 * @param reverse {{{true} if natural order should be reversed
 * @param latitude the latitude
 * @param longitude the longitude
  *
  */
class GeoDistanceSortField( val field : String,
                            val reverse : Boolean,
                            var latitude : Double,
                            var longitude : Double) extends SortField(reverse) {

    if (field == null || StringUtils.isBlank(field)) {
        throw new IndexException("Field name required");
    } else {
        latitude = GeospatialUtils.checkLatitude("latitude", latitude)
        longitude = GeospatialUtils.checkLongitude("longitude", longitude)
    }

    /** @inheritdoc */
    override def sortField(schema : Schema) :  org.apache.lucene.search.SortField = {
        val mapper = schema.mapper(field)
        if (mapper == null) {
            throw new IndexException("Field '{}' is not found", field)
        } else if (!mapper.isInstanceOf[GeoPointMapper]) {
            throw new IndexException("Field '{}' type is not geo_point", field)
        }
        val geoPointMapper = mapper.asInstanceOf[GeoPointMapper]

        val point : Point = CONTEXT.makePoint(longitude, latitude)

        // Use the distance (in km) as source
        val strategy : SpatialStrategy = geoPointMapper.strategy.getGeometryStrategy
        val valueSource : ValueSource = strategy.makeDistanceValueSource(point, DistanceUtils.DEG_TO_KM);
        valueSource.getSortField(reverse)
    }

    /** @inheritdoc */
    override def postProcessingFields : util.Set[String] = Collections.singleton(field)

    /** @inheritdoc */
    override def toString :  String =
        MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("reverse", reverse)
                          .add("latitude", latitude)
                          .add("longitude", longitude)
                          .toString


    /** @inheritdoc */
    override def equals(other: Any) : Boolean = other match {
        case (g: GeoDistanceSortField) => reverse == g.reverse && field.equals(g.field) && latitude == g.latitude && longitude == g.longitude
        case (other) => false
    }

    /** @inheritdoc */
    override def hashCode :Int = {
        var result: Int = field.hashCode
        result = 31 * result + (if (reverse) 1 else 0)
        result = 31 * result + latitude.hashCode
        result = 31 * result + longitude.hashCode
        result
    }
}