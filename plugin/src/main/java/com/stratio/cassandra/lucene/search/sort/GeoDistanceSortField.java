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

package com.stratio.cassandra.lucene.search.sort;

import com.google.common.base.MoreObjects;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SpatialStrategy;

import java.util.Comparator;

/**
 * {@link SortField} to sort geo points by their distance to a fixed reference point.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /** The name of mapper to use to calculate distance. */
    final String mapper;

    /** The longitude of the center point to sort by min distance to it. */
    final double longitude;

    /** The latitude of the center point to sort by min distance to it. */
    final double latitude;

    /**
     * Returns a new {@link SortField}.
     *
     * @param mapper the name of mapper to use to calculate distance
     * @param reverse {@code true} if natural order should be reversed
     * @param longitude the longitude
     * @param latitude the latitude
     */
    public GeoDistanceSortField(String mapper, Boolean reverse, double longitude, double latitude) {
        super(reverse);
        if (mapper == null || StringUtils.isBlank(mapper)) {
            throw new IndexException("Mapper name required");
        }
        this.mapper = mapper;
        this.longitude = GeospatialUtils.checkLongitude("longitude", longitude);
        this.latitude = GeospatialUtils.checkLatitude("latitude", latitude);
    }

    /**
     * Returns the name of mapper to be used to calculate distance.
     *
     * @return the name of mapper
     */
    public String getMapper() {
        return mapper;
    }

    /**
     * Returns the longitude of the reference point to sort by min distance to it.
     *
     * @return the longitude of the reference point
     */
    public double getLongitude() {
        return longitude;
    }

    /**
     * Returns the latitude of the reference point to sort by min distance to it.
     *
     * @return the latitude of the reference point
     */
    public double getLatitude() {
        return latitude;
    }

    /** {@inheritDoc} */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        final Mapper mapper = schema.getMapper(this.mapper);
        if (mapper == null) {
            throw new IndexException("No mapper found for sortFields mapper '%s'", this.mapper);
        } else if (!mapper.sorted) {
            throw new IndexException("Mapper '%s' is not sorted", mapper.field);
        } else if (!(mapper instanceof GeoPointMapper)) {
            throw new IndexException("Only Geo Point Mapper is allowed but Mapper '%s' is not", mapper.field);
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) mapper;

        SpatialStrategy strategy = geoPointMapper.distanceStrategy;
        Point pt = GeoPointMapper.SPATIAL_CONTEXT.makePoint(longitude, latitude);

        // The distance (in km)
        ValueSource valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM);
        return valueSource.getSortField(this.reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<Columns> comparator(Schema schema) {
        final Mapper mapper = schema.getMapper(this.mapper);
        return (Columns o1, Columns o2) -> compare((GeoPointMapper) mapper, o1, o2);
    }

    protected int compare(GeoPointMapper mapper, Columns o1, Columns o2) {

        if (o1 == null) {
            return o2 == null ? 0 : 1;
        } else if (o2 == null) {
            return -1;
        }

        Double longO1 = mapper.readLongitude(o1);
        Double latO1 = mapper.readLatitude(o1);

        Double longO2 = mapper.readLongitude(o2);
        Double latO2 = mapper.readLatitude(o2);

        Double base1 = distance(longO1, latO1);
        Double base2 = distance(longO2, latO2);

        return compare(base1, base2);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("mapper", this.mapper)
                          .add("reverse", reverse)
                          .add("longitude", longitude)
                          .add("latitude", latitude)
                          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoDistanceSortField other = (GeoDistanceSortField) o;
        return reverse == other.reverse &&
               mapper.equals(other.mapper) &&
               longitude == other.longitude &&
               latitude == other.latitude;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = mapper.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        result = 31 * result + new Double(latitude).hashCode();
        result = 31 * result + new Double(longitude).hashCode();
        return result;
    }

    private Double distance(Double oLon, Double oLat) {
        if ((oLon == null) || (oLat == null)) {
            return null;
        }
        return DistanceUtils.distHaversineRAD(DistanceUtils.toRadians(latitude),
                                              DistanceUtils.toRadians(longitude),
                                              DistanceUtils.toRadians(oLat),
                                              DistanceUtils.toRadians(oLon));
    }
}