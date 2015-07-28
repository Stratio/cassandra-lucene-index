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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
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
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

import java.util.Arrays;

/**
 * A {@link Mapper} to map geographical points.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPointMapper extends Mapper {

    public static final SpatialContext spatialContext = SpatialContext.GEO;
    public static final int DEFAULT_MAX_LEVELS = 11;

    private final String latitude;
    private final String longitude;
    private final int maxLevels;

    private final SpatialStrategy strategy;

    /**
     * Builds a new {@link GeoPointMapper}.
     *
     * @param name      The name of the mapper.
     * @param latitude  The name of the column containing the latitude.
     * @param longitude The name of the column containing the longitude.
     * @param maxLevels The maximum number of levels in the tree.
     */
    public GeoPointMapper(String name, String latitude, String longitude, Integer maxLevels) {
        super(name,
              true,
              false,
              Arrays.<AbstractType>asList(AsciiType.instance,
                                          UTF8Type.instance,
                                          Int32Type.instance,
                                          LongType.instance,
                                          IntegerType.instance,
                                          FloatType.instance,
                                          DoubleType.instance,
                                          DecimalType.instance),
              Arrays.asList(latitude, longitude));

        if (StringUtils.isBlank(latitude)) {
            throw new IllegalArgumentException("latitude column name is required");
        }

        if (StringUtils.isBlank(longitude)) {
            throw new IllegalArgumentException("longitude column name is required");
        }

        this.latitude = latitude;
        this.longitude = longitude;
        this.maxLevels = maxLevels == null ? DEFAULT_MAX_LEVELS : maxLevels;
        SpatialPrefixTree grid = new GeohashPrefixTree(spatialContext, this.maxLevels);
        this.strategy = new RecursivePrefixTreeStrategy(grid, name);
    }

    /**
     * Returns the name of the column containing the latitude.
     *
     * @return The name of the column containing the latitude.
     */
    public String getLatitude() {
        return latitude;
    }

    /**
     * Returns the name of the column containing the longitude.
     *
     * @return The name of the column containing the longitude.
     */
    public String getLongitude() {
        return longitude;
    }

    /**
     * Returns the used {@link SpatialStrategy}.
     *
     * @return The used {@link SpatialStrategy}.
     */
    public SpatialStrategy getStrategy() {
        return strategy;
    }

    public int getMaxLevels() {
        return maxLevels;
    }

    @Override
    public void addFields(Document document, Columns columns) {

        Double longitude = readLongitude(columns);
        Double latitude = readLatitude(columns);
        Point point = spatialContext.makePoint(longitude, latitude);

        for (IndexableField field : strategy.createIndexableFields(point)) {
            document.add(field);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new UnsupportedOperationException(String.format("GeoPoint mapper '%s' does not support sorting", name));
    }

    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, latitude);
        validate(metadata, longitude);
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns The {@link Columns} containing the latitude.
     */
    double readLatitude(Columns columns) {
        Column column = columns.getColumnsByName(this.latitude).getFirst();
        if (column == null) {
            throw new IllegalArgumentException("Latitude column required");
        }
        return readLatitude(column.getComposedValue());
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
        return readLongitude(column.getComposedValue());
    }

    /**
     * Returns the latitude contained in the specified {@link Object}. A valid latitude must in the range [-90, 90].
     *
     * @param value The {@link Object} containing the latitude.
     * @return The latitude.
     */
    public static double readLatitude(Object value) {
        Double latitude = null;
        if (value instanceof Number) {
            latitude = ((Number) value).doubleValue();
        } else {
            try {
                latitude = Double.valueOf(value.toString());
            } catch (NumberFormatException e) {
                // Ignore to fail below
            }
        }
        if (latitude == null || latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("Valid latitude required, but found " + value);
        }
        return latitude;
    }

    /**
     * Returns the longitude contained in the specified {@link Object}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param value The {@link Object} containing the latitude.
     * @return The longitude.
     */
    public static double readLongitude(Object value) {
        Double longitude = null;
        if (value instanceof Number) {
            longitude = ((Number) value).doubleValue();
        } else {
            try {
                longitude = Double.valueOf(value.toString());
            } catch (NumberFormatException e) {
                // Ignore to fail below
            }
        }
        if (longitude == null || longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("Valid longitude required, but found " + value);
        }
        return longitude;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("latitude", latitude)
                      .add("longitude", longitude)
                      .add("maxLevels", maxLevels)
                      .toString();
    }
}
