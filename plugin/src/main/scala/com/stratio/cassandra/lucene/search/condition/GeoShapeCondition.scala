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
package com.stratio.cassandra.lucene.search.condition;

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.common.GeoOperation
import com.stratio.cassandra.lucene.common.GeoOperation$
import com.stratio.cassandra.lucene.common.GeoShapes
import com.stratio.cassandra.lucene.common.GeoTransformations
import org.apache.lucene.search.Query
import org.apache.lucene.spatial.SpatialStrategy
import org.apache.lucene.spatial.query.SpatialArgs;

/**
 * [[Condition} that matches documents related to a JTS geographical shape. It is possible to apply a sequence of
 * [[GeoTransformations}s to the provided shape to search for points related to the resulting shape.
 *
 * The shapes are defined using the <a href="http://en.wikipedia.org/wiki/Well-known_text"> Well Known Text (WKT)</a>
 * format.
 *
 * This class depends on <a href="http://www.vividsolutions.com/jts">Java Topology Suite (JTS)</a>. This library can't
 * be distributed together with this project due to license compatibility problems, but you can add it by putting <a
 * href="http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar">jts-core-1.14.0.jar</a>
 * into Cassandra lib directory.
 *
 * Pole wrapping is not supported.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the field name
 * @param shape the shape
 * @param operation the spatial operation to be done, defaults to [[#DEFAULT_OPERATION}
 */
class GeoShapeCondition(val boost : java.lang.Float,
                        val field: String,
                        val shape: GeoShapes.GeoShape,
                        var operation: GeoOperation) extends GeospatialCondition(boost, field, "geo_shape") {

    if (shape == null) throw new IndexException("Shape required")
    operation = if (operation == null) GeoShapeCondition.DEFAULT_OPERATION else operation

    /** @inheritdoc */
    override def doQuery(strategy : SpatialStrategy) : Query = {
        val args : SpatialArgs = new SpatialArgs(operation.spatialOperation, shape.apply)
        args.setDistErr(0.0)
        strategy.makeQuery(args)
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper =
        toStringHelper(this).add("shape", shape).add("operation", operation)

}
object GeoShapeCondition {

    /** The default spatial operation. */
    def DEFAULT_OPERATION : GeoOperation = GeoOperation.IS_WITHIN
}