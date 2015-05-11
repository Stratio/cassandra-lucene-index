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
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoBBoxCondition extends Condition {

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
                            @JsonProperty("min_longitude") double minLongitude,
                            @JsonProperty("max_longitude") double maxLongitude,
                            @JsonProperty("min_latitude") double minLatitude,
                            @JsonProperty("max_latitude") double maxLatitude) {
        super(boost);
        this.field = field;
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {
        GeoRectangle rectangle = new GeoRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);
        return new GeoShapeCondition(boost, field, GeoOperator.Intersects, rectangle).query(schema);
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