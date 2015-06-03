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

import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperBigDecimal;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBigDecimalBuilder extends ColumnMapperBuilder<ColumnMapperBigDecimal> {

    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("integer_digits")
    private Integer integerDigits;

    @JsonProperty("decimal_digits")
    private Integer decimalDigits;

    public ColumnMapperBigDecimalBuilder setIndexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public ColumnMapperBigDecimalBuilder setSorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    public ColumnMapperBigDecimalBuilder setIntegerDigits(Integer integerDigits) {
        this.integerDigits = integerDigits;
        return this;
    }

    public ColumnMapperBigDecimalBuilder setDecimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
        return this;
    }

    @Override
    public ColumnMapperBigDecimal build(String name) {
        return new ColumnMapperBigDecimal(name, indexed, sorted, integerDigits, decimalDigits);
    }
}
