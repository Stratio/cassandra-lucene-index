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
 * {@link SingleColumnMapper} to build a new {@code DateMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateMapper extends SingleColumnMapper<DateMapper> {

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
     * Sets the default date pattern.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified default date pattern
     */
    public DateMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for columns.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified columns date pattern
     */
    public DateMapper columnPattern(String pattern) {
        columnPattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this with the specified fields date pattern
     */
    public DateMapper fieldPattern(String pattern) {
        fieldPattern = pattern;
        return this;
    }

}
