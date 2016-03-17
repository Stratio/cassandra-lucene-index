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
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
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

    public static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;

    /** The name of the latitude column. */
    public final String latitude;

    /** The name of the longitude column. */
    public final String longitude;

    /** The max number of levels in the tree. */
    public final int maxLevels;

    /** The spatial strategy for radial distance searches. */
    public final SpatialStrategy distanceStrategy;

    /** The spatial strategy for bounding box searches. */
    public final SpatialStrategy bboxStrategy;

    /**
     * Builds a new {@link GeoPointMapper}.
     *
     * @param field the name of the field
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @param maxLevels the maximum number of levels in the tree
     */
    public GeoPointMapper(String field,
                          Boolean indexed,
                          Boolean sorted,
                          Boolean validated,
                          String latitude,
                          String longitude,
                          Integer maxLevels) {
        super(field,
              indexed,
              sorted,
              validated,
              null,
              Arrays.asList(latitude, longitude),
              AsciiType.instance,
              DecimalType.instance,
              DoubleType.instance,
              FloatType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              ShortType.instance,
              UTF8Type.instance);

        if (StringUtils.isBlank(latitude)) {
            throw new IndexException("latitude column name is required");
        }

        if (StringUtils.isBlank(longitude)) {
            throw new IndexException("longitude column name is required");
        }

        this.latitude = latitude;
        this.longitude = longitude;
        this.maxLevels = GeospatialUtils.validateGeohashMaxLevels(maxLevels);

        SpatialPrefixTree grid = new GeohashPrefixTree(SPATIAL_CONTEXT, this.maxLevels);
        distanceStrategy = new RecursivePrefixTreeStrategy(grid, field + ".dist");
        bboxStrategy = new BBoxStrategy(SPATIAL_CONTEXT, field + ".bbox");
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {

        Double lon = readLongitude(columns);
        Double lat = readLatitude(columns);

        if (lon == null && lat == null) {
            return;
        } else if (lat == null) {
            throw new IndexException("Latitude column required if there is a longitude");
        } else if (lon == null) {
            throw new IndexException("Longitude column required if there is a latitude");
        }

        Point point = SPATIAL_CONTEXT.makePoint(lon, lat);
        if (indexed) {
            for (IndexableField field : distanceStrategy.createIndexableFields(point)) {
                document.add(field);
            }
            for (IndexableField field : bboxStrategy.createIndexableFields(point)) {
                document.add(field);
            }
        }

        if (sorted) {
            document.add(new StoredField(distanceStrategy.getFieldName(), point.getX() + " " + point.getY()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("GeoPoint mapper '%s' does not support simple sorting", name);
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns the columns containing the latitude
     * @return the validated latitude
     */
    public Double readLatitude(Columns columns) {
        Column<?> column = columns.getColumnsByFullName(latitude).getFirst();
        return column == null ? null : readLatitude(column.getComposedValue());
    }

    /**
     * Returns the longitude contained in the specified {@link Columns}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param columns the columns containing the longitude
     * @return the validated longitude
     */
    public Double readLongitude(Columns columns) {
        Column<?> column = columns.getColumnsByFullName(longitude).getFirst();
        return column == null ? null : readLongitude(column.getComposedValue());
    }

    /**
     * Returns the latitude contained in the specified {@link Object}.
     *
     * A valid latitude must in the range [-90, 90].
     *
     * @param o the {@link Object} containing the latitude
     * @return the latitude
     */
    private Double readLatitude(Object o) {
        Double value;
        if (o == null) {
            return null;
        } else if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable latitude: %s", o);
            }
        }
        return GeospatialUtils.checkLatitude("latitude", value);
    }

    /**
     * Returns the longitude contained in the specified {@link Object}.
     *
     * A valid longitude must in the range [-180, 180].
     *
     * @param o the {@link Object} containing the latitude
     * @return the longitude
     */
    private static Double readLongitude(Object o) {
        Double value;
        if (o == null) {
            return null;
        } else if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable longitude: %s", o);
            }
        }
        return GeospatialUtils.checkLongitude("longitude", value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("validated", validated)
                          .add("latitude", latitude)
                          .add("longitude", longitude)
                          .add("maxLevels", maxLevels)
                          .toString();
    }
}
