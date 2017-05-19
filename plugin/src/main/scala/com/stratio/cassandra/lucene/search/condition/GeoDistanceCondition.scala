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

import com.google.common.base.MoreObjects
import com.spatial4j.core.distance.DistanceUtils
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.common.GeospatialUtils.CONTEXT
import com.stratio.cassandra.lucene.common.{GeoDistance, GeoDistanceUnit, GeospatialUtils}
import org.apache.lucene.search.BooleanClause.Occur.{FILTER, MUST_NOT}
import org.apache.lucene.search.{BooleanQuery, Query}
import org.apache.lucene.spatial.SpatialStrategy
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}

/**
 * A [[Condition]] that matches documents containing a shape contained between two certain circles.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param latitude the latitude of the reference point
 * @param longitude the longitude of the reference point
 * @param minDistance the min allowed distance
 * @param maxDistance the max allowed distance
 */
class GeoDistanceCondition( val boost : java.lang.Float,
                            val field: String,
                            var latitude: Double,
                            var longitude: Double,
                            val minDistance: GeoDistance,
                            val maxDistance: GeoDistance) extends GeospatialCondition(boost, field, "geo_distance") {


    latitude = GeospatialUtils.checkLatitude("latitude",latitude)
    longitude = GeospatialUtils.checkLongitude("longitude",longitude)

    if (maxDistance == null) throw new IndexException("max_distance must be provided")

    if (minDistance != null && minDistance.compare(minDistance, maxDistance) >= 0) throw new IndexException("min_distance must be lower than max_distance")

    /** @inheritdoc */
    override def doQuery(strategy : SpatialStrategy) : Query = {
        val builder : BooleanQuery.Builder = new BooleanQuery.Builder()
        builder.add(query(maxDistance, strategy), FILTER)
        if (minDistance != null) {
            builder.add(query(minDistance, strategy), MUST_NOT)
        }
        builder.build()
    }

    def query( geoDistance : GeoDistance, spatialStrategy : SpatialStrategy) : Query = {
        val kms : Double = geoDistance.getValue(GeoDistanceUnit.KILOMETRES)
        val distance = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM)
        val circle = CONTEXT.makeCircle(longitude, latitude, distance)
        val args = new SpatialArgs(SpatialOperation.Intersects, circle)
        spatialStrategy.makeQuery(args)
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("latitude", latitude)
                                   .add("longitude", longitude)
                                   .add("minDistance", minDistance)
                                   .add("maxDistance", maxDistance)

}