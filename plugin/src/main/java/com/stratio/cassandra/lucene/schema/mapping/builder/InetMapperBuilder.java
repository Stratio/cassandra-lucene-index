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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link InetMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class InetMapperBuilder extends SingleColumnMapperBuilder<InetMapper> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    /**
     * Sets if the field supports searching.
     *
     * @param indexed if the field supports searching.
     * @return This.
     */
    public InetMapperBuilder indexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    /**
     * Sets if the field supports sorting.
     *
     * @param sorted if the field supports sorting.
     * @return This.
     */
    public InetMapperBuilder sorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    /**
     * Returns the {@link InetMapper} represented by this {@link MapperBuilder}.
     *
     * @param name The name of the {@link InetMapper} to be built.
     * @return The {@link InetMapper} represented by this.
     */
    @Override
    public InetMapper build(String name) {
        return new InetMapper(name, column, indexed, sorted);
    }
}
