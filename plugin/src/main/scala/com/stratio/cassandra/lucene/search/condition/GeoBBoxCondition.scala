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
import com.spatial4j.core.shape.Rectangle
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.common.GeospatialUtils
import org.apache.lucene.spatial.SpatialStrategy
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}
import com.stratio.cassandra.lucene.common.GeospatialUtils.CONTEXT
import org.apache.lucene.search.Query

/**
 * A [[Condition]] that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the name of the field to be matched
 * @param minLatitude the minimum accepted latitude
 * @param maxLatitude the maximum accepted latitude
 * @param minLongitude the minimum accepted longitude
 * @param maxLongitude the maximum accepted longitude
 */
class GeoBBoxCondition(  val boost : java.lang.Float,
                                val field: String,
                                var minLatitude: Double,
                                var maxLatitude: Double,
                                var minLongitude: Double,
                                var maxLongitude: Double) extends GeospatialCondition(boost, field, "geo_bbox") {

    minLatitude = GeospatialUtils.checkLatitude("min_latitude", minLatitude)
    maxLatitude = GeospatialUtils.checkLatitude("max_latitude", maxLatitude)
    minLongitude = GeospatialUtils.checkLongitude("min_longitude", minLongitude)
    maxLongitude = GeospatialUtils.checkLongitude("max_longitude", maxLongitude)

    if (minLongitude > maxLongitude) throw new IndexException("min_longitude must be lower than max_longitude")
    if (minLatitude > maxLatitude) throw new IndexException("min_latitude must be lower than max_latitude")



    /** @inheritdoc */
    override def doQuery(strategy : SpatialStrategy) :  Query = {
        val rectangle : Rectangle= CONTEXT.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude)
        val args : SpatialArgs = new SpatialArgs(SpatialOperation.Intersects, rectangle)
        strategy.makeQuery(args)
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("minLatitude", minLatitude)
                                   .add("maxLatitude", maxLatitude)
                                   .add("minLongitude", minLongitude)
                                   .add("maxLongitude", maxLongitude)
}