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
import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a shape with a certain relation with a certain circle.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoShapeCondition extends Condition {

    /** The name of the field to be matched. */
    private final String field;

    /** The name of the field to be matched. */
    private final GeoOperator operator;

    /** The value of the field to be matched. */
    private final GeoShape shape;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *              #DEFAULT_BOOST} is used as default.
     * @param field The name of the field to be matched.
     * @param shape The shape to be matched.
     */
    @JsonCreator
    public GeoShapeCondition(@JsonProperty("boost") Float boost,
                             @JsonProperty("field") String field,
                             @JsonProperty("operator") GeoOperator operator,
                             @JsonProperty("shape") GeoShape shape) {
        super(boost);
        this.field = field;
        this.shape = shape;
        this.operator = operator;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name required");
        }
        if (shape == null) {
            throw new IllegalArgumentException("Geo shape required");
        }
        if (operator == null) {
            throw new IllegalArgumentException("Geo operator required");
        }

        ColumnMapper columnMapper = schema.getMapper(field);
        if (columnMapper == null || !(columnMapper instanceof GeoShapeMapper)) {
            throw new IllegalArgumentException("Not mapper found");
        }
        GeoShapeMapper mapper = (GeoShapeMapper) columnMapper;

        SpatialContext spatialContext = mapper.getSpatialContext();
        SpatialStrategy spatialStrategy = mapper.getStrategy(field);
        SpatialArgs args = new SpatialArgs(operator.getSpatialOperation(), shape.toSpatial4j(spatialContext));
        Query query = spatialStrategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("operator", operator)
                      .add("shape", shape)
                      .toString();
    }
}