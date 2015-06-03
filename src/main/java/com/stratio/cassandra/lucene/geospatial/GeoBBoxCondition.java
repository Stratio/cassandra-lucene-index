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
import com.spatial4j.core.shape.Rectangle;
import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoBBoxCondition extends Condition {

    private static final SpatialContext spatialContext = SpatialContext.GEO;

    private final String field; // The name of the field to be matched
    private final double minLongitude; // The minimum accepted longitude
    private final double maxLongitude; // The maximum accepted longitude
    private final double minLatitude; // The minimum accepted latitude
    private final double maxLatitude; // The maximum accepted latitude

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *              #DEFAULT_BOOST} is used as default.
     * @param field The name of the field to be matched.
     */
    @JsonCreator
    public GeoBBoxCondition(@JsonProperty("boost") Float boost,
                            @JsonProperty("field") String field,
                            @JsonProperty("min_longitude") Double minLongitude,
                            @JsonProperty("max_longitude") Double maxLongitude,
                            @JsonProperty("min_latitude") Double minLatitude,
                            @JsonProperty("max_latitude") Double maxLatitude) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Field name required");
        }

        if (minLongitude == null) {
            throw new IllegalArgumentException("min_longitude required");
        } else if (minLongitude < -180.0 || minLongitude > 180) {
            throw new IllegalArgumentException("min_longitude must be between -180.0 and 180");
        }

        if (maxLongitude == null) {
            throw new IllegalArgumentException("max_longitude required");
        } else if (maxLongitude < -180.0 || maxLongitude > 180) {
            throw new IllegalArgumentException("max_longitude must be between -180.0 and 180");
        }

        if (minLatitude == null) {
            throw new IllegalArgumentException("min_latitude required");
        } else if (minLatitude < -90.0 || minLatitude > 90) {
            throw new IllegalArgumentException("min_latitude must be between -90.0 and 90");
        }

        if (maxLatitude == null) {
            throw new IllegalArgumentException("max_latitude required");
        } else if (maxLatitude < -90.0 || maxLatitude > 90) {
            throw new IllegalArgumentException("max_latitude must be between -90.0 and 90");
        }

        this.field = field;
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
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

        Rectangle rectangle = spatialContext.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);

        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, rectangle);
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("minLongitude", minLongitude)
                      .add("maxLongitude", maxLongitude)
                      .add("minLatitude", minLatitude)
                      .add("maxLatitude", maxLatitude)
                      .toString();
    }
}