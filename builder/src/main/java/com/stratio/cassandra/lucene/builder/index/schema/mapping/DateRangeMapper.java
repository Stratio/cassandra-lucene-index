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
public class DateRangeMapper extends Mapper {

    @JsonProperty("from")
    final String from;

    @JsonProperty("to")
    final String to;

    @JsonProperty("pattern")
    String pattern;

    /**
     * Returns a new {@link DateRangeMapper}.
     *
     * @param from The column containing the from date.
     * @param to   The column containing the to date.
     */
    public DateRangeMapper(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Sets the date pattern to be used.
     *
     * @param pattern The date pattern to be used.
     * @return This.
     */
    public DateRangeMapper pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }
}
