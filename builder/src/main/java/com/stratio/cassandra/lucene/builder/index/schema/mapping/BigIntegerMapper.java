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

import java.math.BigInteger;

/**
 * A {@link Mapper} to map {@link BigInteger} values. A max number of digits must be specified.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapper extends SingleColumnMapper<BigIntegerMapper> {

    /** The max number of digits. */
    @JsonProperty("digits")
    Integer digits;

    /**
     * Sets the max number of digits.
     *
     * @param digits the max number of digits
     * @return this with the specified max number of digits
     */
    public BigIntegerMapper digits(Integer digits) {
        this.digits = digits;
        return this;
    }
}
