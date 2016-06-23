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
package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.util.DateParser;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link DateRangeMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapperBuilder extends MapperBuilder<DateRangeMapper, DateRangeMapperBuilder> {

    /** The column containing the start date. */
    @JsonProperty("from")
    private final String from;

    /** The column containing the stop date. */
    @JsonProperty("to")
    private final String to;

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
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param from the column containing the start date
     * @param to the column containing the stop date
     */
    public DateRangeMapperBuilder(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Sets the default date pattern to be used both for columns and fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this
     */
    public DateRangeMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for columns.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this
     */
    public DateRangeMapperBuilder columnPattern(String pattern) {
        columnPattern = pattern;
        return this;
    }

    /**
     * Sets the date pattern for fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern, or "timestamp" for UNIX time milliseconds
     * @return this
     */
    public DateRangeMapperBuilder fieldPattern(String pattern) {
        fieldPattern = pattern;
        return this;
    }

    /**
     * Returns the {@link DateRangeMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link DateRangeMapper} represented by this
     */
    @Override
    public DateRangeMapper build(String field) {
        DateParser dateParser = new DateParser(pattern, columnPattern, fieldPattern);
        return new DateRangeMapper(field, validated, from, to, dateParser);
    }
}
