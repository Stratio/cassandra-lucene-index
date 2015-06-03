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
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

/**
 * A {@link ColumnMapper} to map geographical points.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoPointMapper extends ColumnMapper {

    private static final SpatialContext spatialContext = SpatialContext.GEO;
    private static final int DEFAULT_MAX_LEVELS = 11;

    private final String latitude;
    private final String longitude;
    private final int maxLevels;
    private final SpatialPrefixTree grid;

    private final SpatialStrategy strategy;

    /**
     * Builds a new {@link GeoPointMapper}.
     *
     * @param name The name of the mapper.
     */
    public GeoPointMapper(String name,
                          Boolean indexed,
                          Boolean sorted,
                          String latitude,
                          String longitude,
                          Integer maxLevels) {
        super(name,
              indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              DecimalType.instance);

        if (StringUtils.isBlank(latitude)) {
            throw new IllegalArgumentException("latitude column name is required");
        }

        if (StringUtils.isBlank(longitude)) {
            throw new IllegalArgumentException("longitude column name is required");
        }

        this.latitude = latitude;
        this.longitude = longitude;
        this.maxLevels = maxLevels == null ? DEFAULT_MAX_LEVELS : maxLevels;
        this.grid = new GeohashPrefixTree(spatialContext, this.maxLevels);
        this.strategy = new RecursivePrefixTreeStrategy(grid, name);
    }

    public SpatialStrategy getStrategy() {
        return strategy;
    }

    @Override
    public void addFields(Document document, Columns columns) {

        Double latitude = readLongitude(columns);
        Double longitude = readLatitude(columns);
        Point point = spatialContext.makePoint(longitude, latitude);

        for (IndexableField field : strategy.createIndexableFields(point)) {
            document.add(field);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(boolean reverse) {
        return new SortField(name, Type.LONG, reverse);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("maxLevels", maxLevels)
                      .add("grid", grid)
                      .add("strategy", strategy)
                      .toString();
    }

    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, latitude);
        validate(metadata, longitude);
    }

    /**
     * Returns the longitude contained in the specified {@link Columns}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param columns The {@link Columns} containing the latitude.
     */
    double readLongitude(Columns columns) {
        Column column = columns.getColumnsByName(this.longitude).getFirst();
        if (column == null) {
            throw new IllegalArgumentException("Longitude column required");
        }
        Object columnValue = column.getComposedValue();
        Double longitude = null;
        if (columnValue != null) {
            if (columnValue instanceof Number) {
                longitude = ((Number) columnValue).doubleValue();
            } else if (columnValue instanceof String) {
                try {
                    longitude = Double.valueOf((String) columnValue);
                } catch (NumberFormatException e) {
                    // Ignore to fail below
                }
            }
        }
        if (longitude == null || longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("Valid longitude required, but found " + latitude);
        }
        return longitude;
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns The {@link Columns} containing the latitude.
     */
    double readLatitude(Columns columns) {
        Column column = columns.getColumnsByName(this.longitude).getFirst();
        if (column == null) {
            throw new IllegalArgumentException("Latitude column required");
        }
        Object columnValue = column.getComposedValue();
        Double latitude = null;
        if (columnValue != null) {
            if (columnValue instanceof Number) {
                latitude = ((Number) columnValue).doubleValue();
            } else if (columnValue instanceof String) {
                try {
                    latitude = Double.valueOf((String) columnValue);
                } catch (NumberFormatException e) {
                    // Ignore to fail below
                }
            }
        }
        if (latitude == null || latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("Valid latitude required, but found " + latitude);
        }
        return latitude;
    }
}
