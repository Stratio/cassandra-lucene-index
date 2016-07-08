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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.MoreObjects;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.core.column.Column;
import com.stratio.cassandra.lucene.core.column.Columns;
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
import scala.Option;

import java.util.Arrays;

import static com.stratio.cassandra.lucene.util.GeospatialUtils.CONTEXT;

/**
 * A {@link Mapper} to map geographical points.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPointMapper extends Mapper {

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
     * @param validated if the field must be validated
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @param maxLevels the maximum number of levels in the tree
     */
    public GeoPointMapper(String field, Boolean validated, String latitude, String longitude, Integer maxLevels) {
        super(field,
              false,
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
              ShortType.instance, UTF8Type.instance);

        if (StringUtils.isBlank(latitude)) {
            throw new IndexException("latitude column name is required");
        }

        if (StringUtils.isBlank(longitude)) {
            throw new IndexException("longitude column name is required");
        }

        this.latitude = latitude;
        this.longitude = longitude;
        this.maxLevels = GeospatialUtils.validateGeohashMaxLevels(maxLevels);

        SpatialPrefixTree grid = new GeohashPrefixTree(CONTEXT, this.maxLevels);
        distanceStrategy = new RecursivePrefixTreeStrategy(grid, field + ".dist");
        bboxStrategy = new BBoxStrategy(CONTEXT, field + ".bbox");
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

        Point point = CONTEXT.makePoint(lon, lat);

        for (IndexableField indexableField : distanceStrategy.createIndexableFields(point)) {
            document.add(indexableField);
        }
        for (IndexableField indexableField : bboxStrategy.createIndexableFields(point)) {
            document.add(indexableField);
        }

        document.add(new StoredField(distanceStrategy.getFieldName(), point.getX() + " " + point.getY()));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("GeoPoint mapper '{}' does not support simple sorting", name);
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns the columns containing the latitude
     * @return the validated latitude
     */
    Double readLatitude(Columns columns) {
        Column<?> column = columns.withFieldName(latitude).head();
        return column == null ? null : readLatitude(column.value());
    }

    /**
     * Returns the longitude contained in the specified {@link Columns}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param columns the columns containing the longitude
     * @return the validated longitude
     */
    Double readLongitude(Columns columns) {
        Column<?> column = columns.withFieldName(longitude).head();
        return column == null ? null : readLongitude(column.value());
    }

    /**
     * Returns the latitude contained in the specified {@link Object}.
     *
     * A valid latitude must in the range [-90, 90].
     *
     * @param option the {@link Object} containing the latitude
     * @return the latitude
     */
    private static <T> Double readLatitude(Option<T> option) {
        Double value;
        if (option == null || option.isEmpty()) {
            return null;
        }
        Object o = option.get();
        if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable latitude: {}", o);
            }
        }
        return GeospatialUtils.checkLatitude("latitude", value);
    }

    /**
     * Returns the longitude contained in the specified {@link Object}.
     *
     * A valid longitude must in the range [-180, 180].
     *
     * @param option the {@link Object} containing the latitude
     * @return the longitude
     */
    private static <T> Double readLongitude(Option<T> option) {
        Double value;
        if (option == null || option.isEmpty()) {
            return null;
        }
        Object o = option.get();
        if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable longitude: {}", o);
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
