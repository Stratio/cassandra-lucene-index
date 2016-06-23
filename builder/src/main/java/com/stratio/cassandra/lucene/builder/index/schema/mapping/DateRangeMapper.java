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

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Mapper} to map 1-dimensional date ranges.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapper extends Mapper<DateRangeMapper> {

    /** The name of the column containing the start date. */
    @JsonProperty("from")
    final String from;

    /** The name of the column containing the end date. */
    @JsonProperty("to")
    final String to;

    /** The default date pattern */
    @JsonProperty("pattern")
    private String pattern;

    /** The date pattern for columns */
    @JsonProperty("column_pattern")
    private String columnPattern;

    /** The date pattern for fields */
    @JsonProperty("field_pattern")
    private String fieldPattern;

    /**
     * Returns a new {@link DateRangeMapper}.
     *
     * @param from the name of the column containing the start date
     * @param to the name of the column containing the stop date
     */
    public DateRangeMapper(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Sets the default date pattern.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern
     * @return this with the specified default date pattern
     */
    public DateRangeMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for columns.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern
     * @return this with the specified columns date pattern
     */
    public DateRangeMapper columnPattern(String pattern) {
        columnPattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern
     * @return this with the specified fields date pattern
     */
    public DateRangeMapper fieldPattern(String pattern) {
        fieldPattern = pattern;
        return this;
    }
}
