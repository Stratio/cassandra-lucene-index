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

package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.GeoBBoxCondition;
import com.stratio.cassandra.lucene.search.condition.GeoPolygonCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link GeoPolygonCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPolygonConditionBuilder extends ConditionBuilder<GeoPolygonCondition, GeoPolygonConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    @JsonProperty("shape")
    private final String shape;

    @JsonCreator
    public GeoPolygonConditionBuilder(@JsonProperty("field") String field,
                                      @JsonProperty("shape") String shape) {
        this.field = field;
        this.shape = shape;
    }

    /**
     * Returns the {@link GeoBBoxCondition} represented by this builder.
     *
     * @return The {@link GeoBBoxCondition} represented by this builder.
     */
    @Override
    public GeoPolygonCondition build() {
        return new GeoPolygonCondition(boost, field, shape);
    }
}
