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

import com.stratio.cassandra.lucene.schema.mapping.BigDecimalMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link BigDecimalMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigDecimalMapperBuilder extends SingleColumnMapperBuilder<BigDecimalMapper> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("integer_digits")
    private Integer integerDigits;

    @JsonProperty("decimal_digits")
    private Integer decimalDigits;

    /**
     * Sets if the field supports searching.
     *
     * @param indexed if the field supports searching.
     * @return This.
     */
    public BigDecimalMapperBuilder indexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    /**
     * Sets if the field supports sorting.
     *
     * @param sorted if the field supports sorting.
     * @return This.
     */
    public BigDecimalMapperBuilder sorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    /**
     * Sets the max number of digits for the integer part.
     *
     * @param integerDigits The max number of digits for the integer part.
     * @return This.
     */
    public BigDecimalMapperBuilder integerDigits(Integer integerDigits) {
        this.integerDigits = integerDigits;
        return this;
    }

    /**
     * Sets the max number of digits for the decimal part.
     *
     * @param decimalDigits The max number of digits for the decimal part.
     * @return This.
     */
    public BigDecimalMapperBuilder decimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
        return this;
    }

    /**
     * Returns the {@link BigDecimalMapper} represented by this {@link MapperBuilder}.
     *
     * @param name The name of the {@link BigDecimalMapper} to be built.
     * @return The {@link BigDecimalMapper} represented by this.
     */
    @Override
    public BigDecimalMapper build(String name) {
        return new BigDecimalMapper(name, column, indexed, sorted, integerDigits, decimalDigits);
    }
}
