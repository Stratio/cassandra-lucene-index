/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import com.stratio.cassandra.lucene.schema.mapping.BigIntegerMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link BigIntegerMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapperBuilder extends SingleColumnMapperBuilder<BigIntegerMapper, BigIntegerMapperBuilder> {

    @JsonProperty("digits")
    private Integer digits;

    /**
     * Sets the max number of digits.
     *
     * @param digits The max number of digits.
     * @return this
     */
    public BigIntegerMapperBuilder digits(Integer digits) {
        this.digits = digits;
        return this;
    }

    /**
     * Returns the {@link BigIntegerMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link BigIntegerMapper} represented by this
     */
    @Override
    public BigIntegerMapper build(String field) {
        return new BigIntegerMapper(field, column, indexed, sorted, validated, digits);
    }
}
