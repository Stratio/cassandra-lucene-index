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

package com.stratio.cassandra.lucene.builder.index.schema.mapping;

import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigDecimal;

/**
 * A {@link Mapper} to map {@link BigDecimal} values. A max number of digits for the integer a decimal parts must be
 * specified.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigDecimalMapper extends SingleColumnMapper<BigDecimalMapper> {

    /** The max number of digits for the integer part. */
    @JsonProperty("integer_digits")
    Integer integerDigits;

    /** The max number of digits for the decimal part. */
    @JsonProperty("decimal_digits")
    Integer decimalDigits;

    /**
     * Sets the max number of digits for the integer part.
     *
     * @param integerDigits the max number of integer digits
     * @return this with the specified max number of integer digits
     */
    public BigDecimalMapper integerDigits(Integer integerDigits) {
        this.integerDigits = integerDigits;
        return this;
    }

    /**
     * Sets the max number of digits for the decimal part.
     *
     * @param decimalDigits the max number of decimal digits
     * @return this with the specified max number of decimal digits
     */
    public BigDecimalMapper decimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
        return this;
    }
}
