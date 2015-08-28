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

import com.google.common.base.Objects;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;

/**
 * A {@link Condition} that matches documents containing a point contained between two certain circles.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoDistanceCondition extends SingleMapperCondition<GeoPointMapper> {

    /** The latitude of the reference point. */
    public final double latitude;

    /** The longitude of the reference point. */
    public final double longitude;

    /** The min allowed distance. */
    public final String minDistance;

    /** The max allowed distance. */
    public final String maxDistance;

    private final GeoDistance minGeoDistance;
    private final GeoDistance maxGeoDistance;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost       The boost for this query clause. Documents matching this clause will (in addition to the
     *                    normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                    #DEFAULT_BOOST} is used as default.
     * @param field       The name of the field to be matched.
     * @param latitude    The latitude of the reference point.
     * @param longitude   The longitude of the reference point.
     * @param minDistance The min allowed distance.
     * @param maxDistance The max allowed distance.
     */
    public GeoDistanceCondition(Float boost,
                                String field,
                                Double latitude,
                                Double longitude,
                                String minDistance,
                                String maxDistance) {
        super(boost, field, GeoPointMapper.class);

        this.latitude = GeoPointMapper.checkLatitude("latitude", latitude);
        this.longitude = GeoPointMapper.checkLongitude("longitude", longitude);

        if (StringUtils.isBlank(maxDistance)) {
            throw new IndexException("max_distance must be provided");
        }

        minGeoDistance = minDistance == null ? null : GeoDistance.create(minDistance);
        maxGeoDistance = GeoDistance.create(maxDistance);

        if (minGeoDistance != null && minGeoDistance.compareTo(maxGeoDistance) >= 0) {
            throw new IndexException("min_distance must be lower than max_distance");
        }

        this.minDistance = minDistance;
        this.maxDistance = maxDistance;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(GeoPointMapper mapper, Analyzer analyzer) {

        SpatialStrategy spatialStrategy = mapper.distanceStrategy;

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(query(maxGeoDistance, spatialStrategy), FILTER);
        if (minGeoDistance != null) {
            builder.add(query(minGeoDistance, spatialStrategy), MUST_NOT);
        }
        Query query = builder.build();
        query.setBoost(boost);
        return query;
    }

    private Query query(GeoDistance geoDistance, SpatialStrategy spatialStrategy) {
        double kms = geoDistance.getValue(GeoDistanceUnit.KILOMETRES);
        double distance = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        Circle circle = GeoPointMapper.SPATIAL_CONTEXT.makeCircle(longitude, latitude, distance);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
        return spatialStrategy.makeQuery(args);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("latitude", latitude)
                                   .add("longitude", longitude)
                                   .add("minDistance", minDistance)
                                   .add("maxDistance", maxDistance)
                                   .toString();
    }

    /**
     * Class representing a geographical distance.
     */
    public static final class GeoDistance implements Comparable<GeoDistance> {

        /** The quantitative distance value. */
        private final double value;

        /** The distance unit. */
        private final GeoDistanceUnit unit;

        /**
         * Builds a new {@link GeoDistance} defined by the specified quantitative value and distance unit.
         *
         * @param value The quantitative distance value.
         * @param unit  The distance unit.
         */
        private GeoDistance(double value, GeoDistanceUnit unit) {
            this.value = value;
            this.unit = unit;
        }

        /**
         * Returns the numeric distance value in the specified unit.
         *
         * @param unit The distance unit to be used.
         * @return The numeric distance value in the specified unit.
         */
        public Double getValue(GeoDistanceUnit unit) {
            return this.unit.getMetres() * value / unit.getMetres();
        }

        /**
         * Returns the {@link GeoDistance} represented by the specified JSON {@code String}.
         *
         * @param json A {@code String} containing a JSON encoded {@link GeoDistance}.
         * @return The {@link GeoDistance} represented by the specified JSON {@code String}.
         */
        @JsonCreator
        public static GeoDistance create(String json) {
            try {
                for (GeoDistanceUnit geoDistanceUnit : GeoDistanceUnit.values()) {
                    for (String name : geoDistanceUnit.getNames()) {
                        if (json.endsWith(name)) {
                            double value = Double.parseDouble(json.substring(0, json.indexOf(name)));
                            return new GeoDistance(value, geoDistanceUnit);
                        }
                    }
                }
                double value = Double.parseDouble(json);
                return new GeoDistance(value, GeoDistanceUnit.METRES);
            } catch (Exception e) {
                throw new IndexException(e, "Unparseable distance: %s", json);
            }
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(GeoDistance other) {
            return getValue(GeoDistanceUnit.MILLIMETRES).compareTo(other.getValue(GeoDistanceUnit.MILLIMETRES));
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("value", value).add("unit", unit).toString();
        }
    }

    /**
     * Enum representing a spatial distance unit.
     */
    public enum GeoDistanceUnit {

        MILLIMETRES(0.001, "mm", "millimetres"),
        CENTIMETRES(0.01, "cm", "centimetres"),
        DECIMETRES(0.1, "dm", "decimetres"),
        DECAMETRES(10, "dam", "decametres"),
        HECTOMETRES(100, "hm", "hectometres"),
        KILOMETRES(1000, "km", "kilometres"),
        FOOTS(0.3048, "ft", "foots"),
        YARDS(0.9144, "yd", "yards"),
        INCHES(0.0254, "in", "inches"),
        MILES(1609.344, "mi", "miles"),
        METRES(1, "m", "metres"),
        NAUTICAL_MILES(1850, "M", "NM", "mil", "nautical_miles");

        private final String[] names;
        private final Double metres;

        /**
         * Builds the {@link GeoDistanceUnit} defined by the specified value in metres and the specified identifying
         * names.
         *
         * @param metres The value in metres.
         * @param names  The identifying names.
         */
        GeoDistanceUnit(double metres, String... names) {
            this.names = names;
            this.metres = metres;
        }

        /**
         * Returns the equivalency in metres.
         *
         * @return The equivalency in metres.
         */
        public Double getMetres() {
            return metres;
        }

        /**
         * Returns the identifying names.
         *
         * @return The identifying names.
         */
        public String[] getNames() {
            return names;
        }

        /**
         * Returns the {@link GeoDistanceUnit} represented by the specified {@code String}.
         *
         * @param value The {@code String} representation of the {@link GeoDistanceUnit} to be created.
         * @return The {@link GeoDistanceUnit} represented by the specified {@code String}.
         */
        @JsonCreator
        public static GeoDistanceUnit create(String value) {
            if (value == null) {
                throw new IllegalArgumentException();
            }
            for (GeoDistanceUnit v : values()) {
                for (String s : v.names) {
                    if (s.equals(value)) {
                        return v;
                    }
                }
            }
            throw new IllegalArgumentException();
        }
    }

}