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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoOperation;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.GeospatialUtilsJTS;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;

import java.util.Collections;
import java.util.List;

/**
 * {@link Condition} that matches documents related to a JTS geographical shape. It is possible to apply a sequence of
 * {@link GeoTransformation}s to the provided shape to search for points related to the resulting shape.
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
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeCondition extends SingleFieldCondition {

    /** The default spatial operation. */
    public static final GeoOperation DEFAULT_OPERATION = GeoOperation.IS_WITHIN;

    /** The spatial context to be used. */
    public static final JtsSpatialContext CONTEXT = JtsSpatialContext.GEO;

    /** The shape. */
    public final JtsGeometry geometry;

    /** The spatial operation to be applied. */
    public final GeoOperation operation;

    /** The sequence of transformations to be applied to the shape before searching. */
    public final List<GeoTransformation> transformations;

    /**
     * Constructor receiving the shape, the spatial operation to be done and the transformations to be applied.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link #DEFAULT_BOOST} is used as
     * default.
     * @param field the field name
     * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
     * @param operation The spatial operation to be done. Defaults to {@link #DEFAULT_OPERATION}.
     * @param transformations the sequence of operations to be applied to the specified shape
     */
    public GeoShapeCondition(Float boost,
                             String field,
                             String shape,
                             GeoOperation operation,
                             List<GeoTransformation> transformations) {
        super(boost, field);
        this.geometry = GeospatialUtilsJTS.geometryFromWKT(CONTEXT, shape);
        this.operation = operation == null ? DEFAULT_OPERATION : operation;
        this.transformations = (transformations == null) ? Collections.<GeoTransformation>emptyList() : transformations;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        // Get the spatial strategy from the mapper
        SpatialStrategy strategy;
        Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '%s'", field);
        } else if (mapper instanceof GeoShapeMapper) {
            strategy = ((GeoShapeMapper) mapper).strategy;
        } else if (mapper instanceof GeoPointMapper) {
            strategy = ((GeoPointMapper) mapper).distanceStrategy;
        } else {
            throw new IndexException("'geo_shape' search requires a mapper of type 'geo_point' or 'geo_shape' " +
                                     "but found %s:%s", field, mapper);
        }

        // Apply transformations
        JtsGeometry transformedGeometry = geometry;
        if (transformations != null) {
            for (GeoTransformation transformation : transformations) {
                transformedGeometry = transformation.apply(transformedGeometry, CONTEXT);
            }
        }

        // Build query
        SpatialArgs args = new SpatialArgs(operation.getSpatialOperation(), transformedGeometry);
        args.setDistErr(0.0);
        Query query = strategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("geometry", geometry)
                                   .add("operation", operation)
                                   .add("transformations", transformations)
                                   .toString();
    }
}