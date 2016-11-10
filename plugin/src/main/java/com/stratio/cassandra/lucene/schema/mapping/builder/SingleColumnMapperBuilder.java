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
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;

/**
 * Abstract {@link MapperBuilder} for creating new {@link SingleColumnMapper}s.
 *
 * @param <T> The {@link SingleColumnMapper} to be built.
 * @param <K> The specific {@link SingleColumnMapper}.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapperBuilder<T extends SingleColumnMapper<?>, K extends SingleColumnMapperBuilder<T, K>>
        extends MapperBuilder<T, K> {

    /** The name of the column to be mapped. */
    @JsonProperty("column")
    protected String column;

    /**
     * Sets the name of the Cassandra column to be mapped.
     *
     * @param column the name of the Cassandra column to be mapped
     * @return this
     */
    @SuppressWarnings("unchecked")
    public final K column(String column) {
        this.column = column;
        return (K) this;
    }
}
