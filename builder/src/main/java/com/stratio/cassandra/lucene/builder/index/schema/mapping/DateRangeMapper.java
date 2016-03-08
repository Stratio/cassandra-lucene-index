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

    /** The date pattern to be used. */
    @JsonProperty("pattern")
    String pattern;

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
     * Sets the date pattern to be used.
     *
     * @param pattern the date pattern to be used
     * @return this with the specified date pattern
     */
    public DateRangeMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }
}
