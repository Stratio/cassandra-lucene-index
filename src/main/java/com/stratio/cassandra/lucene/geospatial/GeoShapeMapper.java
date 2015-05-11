/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.geospatial;

import com.google.common.base.Objects;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link ColumnMapper} to map geographical shapes.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoShapeMapper extends ColumnMapper {

    public static final SpatialContext spatialContext = SpatialContext.GEO;
    public static final int DEFAULT_MAX_LEVELS = 11;

    private final int maxLevels;
    private final SpatialPrefixTree grid;

    private final Map<String, SpatialStrategy> strategies = new HashMap<>();

    /**
     * Builds a new {@link GeoShapeMapper}.
     */
    @JsonCreator
    public GeoShapeMapper(@JsonProperty("indexed") Boolean indexed,
                          @JsonProperty("sorted") Boolean sorted,
                          @JsonProperty("max_levels") Integer maxLevels) {
        super(indexed, sorted, new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance});
        this.maxLevels = maxLevels == null ? DEFAULT_MAX_LEVELS : maxLevels;
        this.grid = new GeohashPrefixTree(spatialContext, this.maxLevels);
    }

    public void addFields(Document document, String name, Object value, boolean isCollection) {
        SpatialStrategy strategy = getStrategy(name);
        GeoShape geoShape = GeoShape.fromJson((String) value);
        Shape shape = geoShape.toSpatial4j(spatialContext);
        for (IndexableField field : strategy.createIndexableFields(shape)) {
            document.add(field);
        }
    }

    public SpatialStrategy getStrategy(String fieldName) {
        SpatialStrategy strategy = strategies.get(fieldName);
        if (strategy == null) {
            strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
            strategies.put(fieldName, strategy);
        }
        return strategy;
    }

    public SpatialContext getSpatialContext() {
        return spatialContext;
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.LONG, reverse);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("maxLevels", maxLevels)
                      .add("grid", grid)
                      .add("strategies", strategies)
                      .toString();
    }
}
