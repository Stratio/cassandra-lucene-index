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
package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.Objects;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

/**
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoBBoxCondition extends Condition {

    /** The name of the field to be matched. */
    public final String field;

    /** The minimum accepted latitude. */
    public final double minLatitude;

    /** The maximum accepted latitude. */
    public final double maxLatitude;

    /** The minimum accepted longitude. */
    public final double minLongitude;

    /** The maximum accepted longitude. */
    public final double maxLongitude;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost        The boost for this query clause. Documents matching this clause will (in addition to the
     *                     normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                     #DEFAULT_BOOST} is used as default.
     * @param field        The name of the field to be matched.
     * @param minLatitude  The minimum accepted latitude.
     * @param maxLatitude  The maximum accepted latitude.
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     */
    public GeoBBoxCondition(Float boost,
                            String field,
                            Double minLatitude,
                            Double maxLatitude,
                            Double minLongitude,
                            Double maxLongitude) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Field name required");
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

        if (minLongitude > maxLongitude) {
            throw new IllegalArgumentException("min_longitude must be lower than max_longitude");
        }

        if (minLatitude > maxLatitude) {
            throw new IllegalArgumentException("min_latitude must be lower than max_latitude");
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

        Mapper mapper = schema.getMapper(field);
        if (!(mapper instanceof GeoPointMapper)) {
            throw new IllegalArgumentException("Geo point mapper required");
        }
        GeoPointMapper geoPointMapper = (GeoPointMapper) mapper;
        SpatialStrategy spatialStrategy = geoPointMapper.getBBoxStrategy();

        SpatialContext context = GeoPointMapper.SPATIAL_CONTEXT;
        Rectangle rectangle = context.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);

        SpatialArgs args = new SpatialArgs(SpatialOperation.BBoxIntersects, rectangle);
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("boost", boost)
                      .add("field", field)
                      .add("minLatitude", minLatitude)
                      .add("maxLatitude", maxLatitude)
                      .add("minLongitude", minLongitude)
                      .add("maxLongitude", maxLongitude)
                      .toString();
    }
}