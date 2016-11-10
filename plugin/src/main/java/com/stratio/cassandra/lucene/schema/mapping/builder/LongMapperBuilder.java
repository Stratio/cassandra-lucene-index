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
package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link LongMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LongMapperBuilder extends SingleColumnMapperBuilder<LongMapper, LongMapperBuilder> {

    @JsonProperty("boost")
    private Float boost;

    /**
     * Sets the boost to be used.
     *
     * @param boost the boost to be used
     * @return this
     */
    public LongMapperBuilder boost(Float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Returns the {@link LongMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link LongMapper} represented by this
     */
    @Override
    public LongMapper build(String field) {
        return new LongMapper(field, column, validated, boost);
    }
}
