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

package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.MoreObjects;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import com.stratio.cassandra.lucene.util.GeospatialUtilsJTS;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Mapper} to map geographical shapes represented according to the <a href="http://en.wikipedia.org/wiki/Well-known_text">
 * Well Known Text (WKT)</a> format.
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
public class GeoShapeMapper extends SingleColumnMapper<String> {

    public static final JtsSpatialContext SPATIAL_CONTEXT = JtsSpatialContext.GEO;

    /** The name of the mapped column. */
    public final String column;

    /** The max number of levels in the tree. */
    public final int maxLevels;

    /** The spatial strategy for radial distance searches. */
    public final RecursivePrefixTreeStrategy strategy;

    /** The sequence of transformations to be applied to the shape before indexing. */
    public final List<GeoTransformation> transformations;

    /**
     * Builds a new {@link GeoShapeMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column
     * @param validated if the field must be validated
     * @param maxLevels the maximum number of levels in the tree
     * @param transformations the sequence of operations to be applied to the specified shape
     */
    public GeoShapeMapper(String field,
                          String column,
                          Boolean validated,
                          Integer maxLevels,
                          List<GeoTransformation> transformations) {
        super(field, column, true, false, validated, null, String.class, AsciiType.instance, UTF8Type.instance);

        this.column = column == null ? field : column;

        if (StringUtils.isWhitespace(column)) {
            throw new IndexException("Column must not be whitespace, but found '%s'", column);
        }

        this.maxLevels = GeospatialUtils.validateGeohashMaxLevels(maxLevels);
        SpatialPrefixTree grid = new GeohashPrefixTree(SPATIAL_CONTEXT, this.maxLevels);
        strategy = new RecursivePrefixTreeStrategy(grid, field);

        this.transformations = (transformations == null) ? Collections.<GeoTransformation>emptyList() : transformations;
    }

    /** {@inheritDoc} */
    @Override
    public void addIndexedFields(Document document, String name, String value) {

        // Parse shape
        JtsGeometry shape = GeospatialUtilsJTS.geometryFromWKT(SPATIAL_CONTEXT, value);

        // Apply transformations
        if (transformations != null) {
            for (GeoTransformation transformation : transformations) {
                shape = transformation.apply(shape, SPATIAL_CONTEXT);
            }
        }

        // Add fields
        for (IndexableField field : strategy.createIndexableFields(shape)) {
            document.add(field);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addSortedFields(Document document, String name, String value) {
        // Nothing to do here
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("GeoShape mapper '%s' does not support simple sorting", name);
    }

    protected String doBase(String field, @NotNull Object value) {
        return value.toString();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("column", column)
                          .add("validated", validated)
                          .add("maxLevels", maxLevels)
                          .add("transformations", transformations)
                          .toString();
    }

}
