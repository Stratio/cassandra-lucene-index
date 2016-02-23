/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.condition;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.io.jts.JtsWktShapeParser;
import com.spatial4j.core.shape.Shape;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.text.ParseException;

/**
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPolygonCondition extends SingleMapperCondition<GeoPointMapper> {

    /** The list of shape in WKT format. */
    public final String shape;

    public GeoPolygonCondition(Float boost,
                               String field,
                               String shape) {
        super(boost, field, GeoPointMapper.class);

        this.shape = shape;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(GeoPointMapper mapper, Analyzer analyzer) {

        SpatialStrategy spatialStrategy = mapper.distanceStrategy;

//        SpatialContext context = GeoPointMapper.SPATIAL_CONTEXT;

        Shape shape;
        try {

            shape = JtsSpatialContext.GEO.getWktShapeParser().parse(this.shape);

        } catch (ParseException e) {
            throw new IndexException("Shape is not parseable", e);
        }
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);
        args.setDistErr(0.0);
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("shape", shape).toString();
    }
}