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

}