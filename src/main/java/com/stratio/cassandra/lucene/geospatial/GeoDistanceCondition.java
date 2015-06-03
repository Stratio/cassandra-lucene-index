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
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a point contained between two certain circles.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceCondition extends Condition {

    private static final SpatialContext spatialContext = SpatialContext.GEO;

    private final String field; // The name of the field to be matched.
    private final double longitude;
    private final double latitude;
    private final GeoDistance minDistance;
    private final GeoDistance maxDistance;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *              #DEFAULT_BOOST} is used as default.
     * @param field The name of the field to be matched.
     */
    @JsonCreator
    public GeoDistanceCondition(@JsonProperty("boost") Float boost,
                                @JsonProperty("field") String field,
                                @JsonProperty("longitude") Double longitude,
                                @JsonProperty("latitude") Double latitude,
                                @JsonProperty("min_distance") GeoDistance minDistance,
                                @JsonProperty("max_distance") GeoDistance maxDistance) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Field name required");
        }

        if (longitude == null) {
            throw new IllegalArgumentException("longitude required");
        } else if (longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("longitude must be between -180.0 and 180");
        }

        if (latitude == null) {
            throw new IllegalArgumentException("latitude required");
        } else if (latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("latitude must be between -90.0 and 90");
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

        ColumnMapper columnMapper = schema.getMapper(field);
        if (!(columnMapper instanceof GeoPointMapper)) {
            throw new IllegalArgumentException("Geo point mapper required");
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) columnMapper;
        SpatialStrategy spatialStrategy = geoPointMapper.getStrategy();

        if (minDistance != null && maxDistance != null) {
            BooleanQuery query = new BooleanQuery();
            query.add(maxQuery(spatialStrategy), BooleanClause.Occur.MUST);
            query.add(minQuery(spatialStrategy), BooleanClause.Occur.MUST_NOT);
            return query;
        } else if (minDistance == null && maxDistance != null) {
            return maxQuery(spatialStrategy);
        } else if (minDistance != null) {
            return minQuery(spatialStrategy);
        } else {
            throw new IllegalArgumentException("min_distance and/or max_distance required");
        }
    }

    Query minQuery(SpatialStrategy spatialStrategy) {
        double kms = minDistance.getValue(GeoDistanceUnit.KILOMETRES);
        double distance = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        Circle circle = spatialContext.makeCircle(longitude, latitude, distance);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    Query maxQuery(SpatialStrategy spatialStrategy) {
        double kms = maxDistance.getValue(GeoDistanceUnit.KILOMETRES);
        double distance = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        Circle circle = spatialContext.makeCircle(longitude, latitude, distance);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("longitude", longitude)
                      .add("latitude", latitude)
                      .add("minDistance", minDistance)
                      .add("maxDistance", maxDistance)
                      .toString();
    }
}