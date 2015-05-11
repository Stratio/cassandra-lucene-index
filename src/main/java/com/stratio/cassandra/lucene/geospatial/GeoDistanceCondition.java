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
import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a shape contained in a certain circle.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceCondition extends Condition {

    private final String field; // The name of the field to be matched.
    private final double longitude; // The longitude of the circle's center
    private final double latitude; // The latitude of the circle's center
    private final GeoDistance distance; // The radius of the circle

    /**
     * @param boost     The boost for this query clause. Documents matching this clause will (in addition to the normal
     *                  weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                  #DEFAULT_BOOST} is used as default.
     * @param field     The name of the field to be matched.
     * @param longitude The longitude of the circle's center.
     * @param latitude  The latitude of the circle's center.
     * @param distance  The radius of the circle.
     */
    @JsonCreator
    public GeoDistanceCondition(@JsonProperty("boost") Float boost,
                                @JsonProperty("field") String field,
                                @JsonProperty("longitude") double longitude,
                                @JsonProperty("latitude") double latitude,
                                @JsonProperty("distance") GeoDistance distance) {
        super(boost);
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.distance = distance;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {
        GeoCircle circle = new GeoCircle(longitude, latitude, distance);
        return new GeoShapeCondition(boost, field, GeoOperator.Intersects, circle).query(schema);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("longitude", longitude)
                      .add("latitude", latitude)
                      .add("distance", distance)
                      .toString();
    }
}