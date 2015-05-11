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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a shape contained between two certain circles.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceRangeCondition extends Condition {

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
    public GeoDistanceRangeCondition(@JsonProperty("boost") Float boost,
                                     @JsonProperty("field") String field,
                                     @JsonProperty("longitude") double longitude,
                                     @JsonProperty("latitude") double latitude,
                                     @JsonProperty("min_distance") GeoDistance minDistance,
                                     @JsonProperty("max_distance") GeoDistance maxDistance) {
        super(boost);
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.minDistance = minDistance;
        this.maxDistance = maxDistance;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        GeoCircle minCircle = new GeoCircle(longitude, latitude, minDistance);
        GeoCircle maxCircle = new GeoCircle(longitude, latitude, maxDistance);

        Condition minCondition = new GeoShapeCondition(boost, field, GeoOperator.IsDisjointTo, minCircle);
        Condition maxCondition = new GeoShapeCondition(boost, field, GeoOperator.Intersects, maxCircle);

        BooleanQuery query = new BooleanQuery();
        query.add(minCondition.query(schema), BooleanClause.Occur.MUST);
        query.add(maxCondition.query(schema), BooleanClause.Occur.MUST);
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