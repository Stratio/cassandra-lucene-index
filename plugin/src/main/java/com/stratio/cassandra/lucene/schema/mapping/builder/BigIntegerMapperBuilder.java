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

import com.stratio.cassandra.lucene.schema.mapping.BigIntegerMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapperBuilder extends MapperBuilder<BigIntegerMapper> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("digits")
    private Integer digits;

    /**
     * Sets if the field supports searching.
     *
     * @param indexed if the field supports searching.
     * @return This.
     */
    public BigIntegerMapperBuilder indexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    /**
     * Sets if the field supports sorting.
     *
     * @param sorted if the field supports sorting.
     * @return This.
     */
    public BigIntegerMapperBuilder sorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    /**
     * Sets the max number of digits.
     *
     * @param digits The max number of digits.
     * @return This.
     */
    public BigIntegerMapperBuilder digits(Integer digits) {
        this.digits = digits;
        return this;
    }

    /**
     * Returns the {@link BigIntegerMapper} represented by this {@link MapperBuilder}.
     *
     * @param name The name of the {@link BigIntegerMapper} to be built.
     * @return The {@link BigIntegerMapper} represented by this.
     */
    @Override
    public BigIntegerMapper build(String name) {
        return new BigIntegerMapper(name, indexed, sorted, digits);
    }
}
