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

import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link DoubleMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapperBuilder extends SingleColumnMapperBuilder<DoubleMapper, DoubleMapperBuilder> {

    @JsonProperty("boost")
    private Float boost;

    /**
     * Sets the boost to be used.
     *
     * @param boost the boost to be used
     * @return this
     */
    public DoubleMapperBuilder boost(Float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Returns the {@link DoubleMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link DoubleMapper} represented by this
     */
    @Override
    public DoubleMapper build(String field) {
        return new DoubleMapper(field, column, indexed, sorted, validated, boost);
    }
}
