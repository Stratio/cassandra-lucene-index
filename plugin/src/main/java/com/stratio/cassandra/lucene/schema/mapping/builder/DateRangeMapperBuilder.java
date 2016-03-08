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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link DateRangeMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapperBuilder extends MapperBuilder<DateRangeMapper, DateRangeMapperBuilder> {

    @JsonProperty("from")
    private final String from;

    @JsonProperty("to")
    private final String to;

    @JsonProperty("pattern")
    private String pattern;

    /**
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param from he column containing the from date
     * @param to the column containing the to date
     */
    public DateRangeMapperBuilder(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Sets the date pattern to be used.
     *
     * @param pattern the date pattern to be used
     * @return this
     */
    public DateRangeMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
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
        return new DateRangeMapper(field, validated, from, to, pattern);
    }
}
