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

    /** The default date pattern */
    @JsonProperty("pattern")
    private String pattern;

    /** The date pattern for columns */
    @JsonProperty("column_pattern")
    private String columnPattern;

    /** The date pattern for fields */
    @JsonProperty("field_pattern")
    private String fieldPattern;

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
     * Sets the default date pattern.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified default date pattern
     */
    public BitemporalMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for columns.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified columns date pattern
     */
    public BitemporalMapper columnPattern(String pattern) {
        columnPattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified fields date pattern
     */
    public BitemporalMapper fieldPattern(String pattern) {
        fieldPattern = pattern;
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
