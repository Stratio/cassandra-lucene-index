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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link IntegerMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IntegerMapperBuilder extends SingleColumnMapperBuilder<IntegerMapper, IntegerMapperBuilder> {

    @JsonProperty("boost")
    private Float boost;

    /**
     * Sets the boost to be used.
     *
     * @param boost The boost to be used.
     * @return This.
     */
    public IntegerMapperBuilder boost(Float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Returns the {@link IntegerMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link IntegerMapper} represented by this
     */
    @Override
    public IntegerMapper build(String field) {
        return new IntegerMapper(field, column, indexed, sorted, validated, boost);
    }
}
