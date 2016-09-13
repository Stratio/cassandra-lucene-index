/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.common.GeoOperation;
import com.stratio.cassandra.lucene.common.GeoShape;
import com.stratio.cassandra.lucene.common.JTSNotFoundException;
import com.stratio.cassandra.lucene.search.condition.GeoBBoxCondition;
import com.stratio.cassandra.lucene.search.condition.GeoShapeCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link GeoShapeCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeConditionBuilder extends ConditionBuilder<GeoShapeCondition, GeoShapeConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format. */
    @JsonProperty("shape")
    private final GeoShape shape;

    /** The spatial operation to be applied. */
    @JsonProperty("operation")
    private GeoOperation operation;

    /**
     * Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
     */
    @JsonCreator
    public GeoShapeConditionBuilder(@JsonProperty("field") String field, @JsonProperty("shape") GeoShape shape) {
        this.field = field;
        this.shape = shape;
    }

    /**
     * Sets the name of the spatial operation to be performed.
     *
     * @param operation the name of the spatial operation
     * @return this with the operation set
     */
    @JsonProperty("operation")
    public GeoShapeConditionBuilder operation(String operation) {
        this.operation = GeoOperation.parse(operation);
        return this;
    }

    /**
     * Sets the name of the spatial operation to be performed.
     *
     * @param operation the name of the spatial operation
     * @return this with the operation set
     */
    public GeoShapeConditionBuilder operation(GeoOperation operation) {
        this.operation = operation;
        return this;
    }

    /**
     * Returns the {@link GeoBBoxCondition} represented by this builder.
     *
     * @return a new geo shape condition
     */
    @Override
    public GeoShapeCondition build() {
        try {
            return new GeoShapeCondition(boost, field, shape, operation);
        } catch (NoClassDefFoundError e) {
            throw new JTSNotFoundException();

        }
    }
}
