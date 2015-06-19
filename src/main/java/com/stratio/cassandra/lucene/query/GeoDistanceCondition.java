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
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.GeoDistance;
import com.stratio.cassandra.lucene.util.GeoDistanceUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a point contained between two certain circles.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceCondition extends Condition {

    static final SpatialContext spatialContext = SpatialContext.GEO;

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The latitude of the reference point. */
    @JsonProperty("latitude")
    final double latitude;

    /** The longitude of the reference point. */
    @JsonProperty("longitude")
    final double longitude;

    /** The min allowed distance. */
    @JsonProperty("min_distance")
    final String minDistance;

    /** The max allowed distance. */
    @JsonProperty("max_distance")
    final String maxDistance;

    @JsonIgnore
    final GeoDistance minGeoDistance;

    @JsonIgnore
    final GeoDistance maxGeoDistance;

    /**
     * Constructor using the field name and the value to be matched.
     *  @param boost      The boost for this query clause. Documents matching this clause will (in addition to the
     *                    normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                    #DEFAULT_BOOST} is used as default.
     * @param field       The name of the field to be matched.
     * @param latitude    The latitude of the reference point.
     * @param longitude   The longitude of the reference point.
     * @param minDistance The min allowed distance.
     * @param maxDistance The max allowed distance.
     */
    @JsonCreator
    public GeoDistanceCondition(@JsonProperty("boost") Float boost,
                                @JsonProperty("field") String field,
                                @JsonProperty("latitude") Double latitude,
                                @JsonProperty("longitude") Double longitude,
                                @JsonProperty("min_distance") String minDistance,
                                @JsonProperty("max_distance") String maxDistance) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Field name required");
        }

        if (latitude == null) {
            throw new IllegalArgumentException("latitude required");
        } else if (latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("latitude must be between -90.0 and 90.0");
        }

        if (longitude == null) {
            throw new IllegalArgumentException("longitude required");
        } else if (longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("longitude must be between -180.0 and 180.0");
        }

        if (StringUtils.isBlank(maxDistance)) {
            throw new IllegalArgumentException("max_distance must be provided");
        }

        minGeoDistance = minDistance == null ? null : GeoDistance.create(minDistance);
        maxGeoDistance = GeoDistance.create(maxDistance);

        if (minGeoDistance != null && minGeoDistance.compareTo(maxGeoDistance) >= 0) {
            throw new IllegalArgumentException("min_distance must be lower than max_distance");
        }

        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.minDistance = minDistance;
        this.maxDistance = maxDistance;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        Mapper mapper = schema.getMapper(field);
        if (!(mapper instanceof GeoPointMapper)) {
            throw new IllegalArgumentException("Geo point mapper required");
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) mapper;
        SpatialStrategy spatialStrategy = geoPointMapper.getStrategy();

        BooleanQuery query = new BooleanQuery();
        query.add(query(maxGeoDistance, spatialStrategy), BooleanClause.Occur.MUST);
        if (minGeoDistance != null) {
            query.add(query(minGeoDistance, spatialStrategy), BooleanClause.Occur.MUST_NOT);
        }
        query.setBoost(boost);
        return query;
    }

    private Query query(GeoDistance geoDistance, SpatialStrategy spatialStrategy) {
        double kms = geoDistance.getValue(GeoDistanceUnit.KILOMETRES);
        double distance = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        Circle circle = spatialContext.makeCircle(longitude, latitude, distance);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
        return spatialStrategy.makeQuery(args);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("latitude", latitude)
                      .add("longitude", longitude)
                      .add("minDistance", minDistance)
                      .add("maxDistance", maxDistance)
                      .toString();
    }
}