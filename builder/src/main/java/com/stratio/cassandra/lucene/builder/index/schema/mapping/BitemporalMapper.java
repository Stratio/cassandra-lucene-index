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

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Mapper} to map bitemporal DateRanges.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapper extends Mapper<BitemporalMapper> {

    /** The name of the column containing the valid time start. **/
    @JsonProperty("vt_from")
    final String vtFrom;

    /** The name of the column containing the valid time stop. **/
    @JsonProperty("vt_to")
    final String vtTo;

    /** The name of the column containing the transaction time start. **/
    @JsonProperty("tt_from")
    final String ttFrom;

    /** The name of the column containing the transaction time stop. **/
    @JsonProperty("tt_to")
    final String ttTo;

    /** The date pattern. **/
    @JsonProperty("pattern")
    String pattern;

    /** The NOW Value. **/
    @JsonProperty("now_value")
    Object nowValue;

    /**
     * Returns a new {@link BitemporalMapper}.
     *
     * @param vtFrom the column name containing the valid time start
     * @param vtTo the column name containing the valid time stop
     * @param ttFrom the column name containing the transaction time start
     * @param ttTo the column name containing the transaction time stop
     */
    @JsonCreator
    public BitemporalMapper(@JsonProperty("vt_from") String vtFrom,
                            @JsonProperty("vt_to") String vtTo,
                            @JsonProperty("tt_from") String ttFrom,
                            @JsonProperty("tt_to") String ttTo) {
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    /**
     * Sets the date format pattern to be used.
     *
     * @param pattern the date pattern
     * @return this with the specified date pattern
     */
    public BitemporalMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the now value to be used.
     *
     * @param nowValue the now value
     * @return this with the specified now value
     */
    public BitemporalMapper nowValue(Object nowValue) {
        this.nowValue = nowValue;
        return this;
    }
}
