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

import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperBigInteger;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBigIntegerBuilder extends ColumnMapperBuilder<ColumnMapperBigInteger> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("digits")
    private Integer digits;

    public ColumnMapperBigIntegerBuilder setIndexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public ColumnMapperBigIntegerBuilder setSorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    public ColumnMapperBigIntegerBuilder setDigits(Integer digits) {
        this.digits = digits;
        return this;
    }

    @Override
    public ColumnMapperBigInteger build(String name) {
        return new ColumnMapperBigInteger(name, indexed, sorted, digits);
    }
}
