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

package com.stratio.cassandra.lucene.builder.search.condition;

import com.stratio.cassandra.lucene.builder.common.GeoTransformation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Condition} for building a new {@link GeoShapeCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeCondition extends Condition {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format. */
    @JsonProperty("shape")
    final String shape;

    /** The spatial operation to be applied. */
    @JsonProperty("operation")
    String operation;

    /** The sequence of transformations to be applied to the shape before searching. */
    @JsonProperty("transformations")
    List<GeoTransformation> transformations;

    /**
     * Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
     */
    @JsonCreator
    public GeoShapeCondition(@JsonProperty("field") String field, @JsonProperty("shape") String shape) {
        this.field = field;
        this.shape = shape;
    }

    /**
     * Sets the name of the spatial operation to be performed.
     *
     * @param operation the name of the spatial operation
     * @return this with the operation set
     */
    public GeoShapeCondition operation(String operation) {
        this.operation = operation;
        return this;
    }

    /**
     * Sets the transformations to be applied to the shape before using it for indexing it. Possible values are {@code
     * intersects}, {@code is_within} and {@code contains}. Defaults to {@code is_within}.
     *
     * @param transformations the sequence of transformations
     * @return this with the transformations set
     */
    public GeoShapeCondition transform(GeoTransformation... transformations) {
        if (this.transformations == null) {
            this.transformations = Arrays.asList(transformations);
        } else {
            this.transformations.addAll(Arrays.asList(transformations));
        }
        return this;
    }
}