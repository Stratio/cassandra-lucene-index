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
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;

/**
 * {@link Condition} that matches documents related to a JTS geographical shape. It is possible to apply a sequence of
 * {@link GeoTransformation}s to the provided shape to search for points related to the resulting shape.
 *
 * The shapes are defined using the <a href="http://en.wikipedia.org/wiki/Well-known_text"> Well Known Text (WKT)</a>
 * format.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeCondition extends SingleMapperCondition<GeoPointMapper> {

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
     * @param operation The spatial operation to be done.  If {@code null}, then {@link #DEFAULT_OPERATION} is used as
     * default.
     * @param transformations the sequence of operations to be applied to the specified shape
     */
    public GeoShapeCondition(Float boost,
                             String field,
                             String shape,
                             GeoOperation operation,
                             List<GeoTransformation> transformations) {
        super(boost, field, GeoPointMapper.class);
        this.geometry = geometryFromWKT(shape);
        this.operation = operation == null ? DEFAULT_OPERATION : operation;
        this.transformations = (transformations == null) ? Collections.<GeoTransformation>emptyList() : transformations;
    }

    /**
     * Returns the {@link JtsGeometry} represented by the specified WKT text.
     *
     * @param string the WKT text
     * @return the parsed geometry
     */
    protected static JtsGeometry geometryFromWKT(String string) {
        if (StringUtils.isBlank(string)) {
            throw new IndexException("Shape shouldn't be blank");
        }
        try {
            Shape shape = CONTEXT.getWktShapeParser().parse(string);
            return CONTEXT.makeShape(CONTEXT.getGeometryFrom(shape));
        } catch (ParseException e) {
            throw new IndexException(e, "Shape '%s' is not parseable", string);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Query query(GeoPointMapper mapper, Analyzer analyzer) {

        SpatialStrategy strategy = mapper.distanceStrategy;

        // Apply transformations
        logger.debug("INITIAL SHAPE: {}", geometry);
        JtsGeometry transformedGeometry = geometry;
        if (transformations != null) {
            for (GeoTransformation transformation : transformations) {
                transformedGeometry = transformation.apply(transformedGeometry, CONTEXT);
                logger.debug("TRANSFORMED TO: {}", transformedGeometry);
            }
        }

        // Build query
        SpatialArgs args = new SpatialArgs(operation.getSpatialOperation(), geometry);
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