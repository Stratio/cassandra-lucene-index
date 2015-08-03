/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for building a new {@link DateRangeMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapperBuilder extends MapperBuilder<DateRangeMapper> {

    @JsonProperty("start")
    private final String start;

    @JsonProperty("stop")
    private final String stop;

    @JsonProperty("pattern")
    private String pattern;

    /**
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param start The column containing the start date.
     * @param stop  The column containing the stop date.
     */
    public DateRangeMapperBuilder(@JsonProperty("start") String start, @JsonProperty("stop") String stop) {
        this.start = start;
        this.stop = stop;
    }

    public DateRangeMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Returns the {@link DateRangeMapper} represented by this {@link MapperBuilder}.
     *
     * @param name The name of the {@link DateRangeMapper} to be built.
     * @return The {@link DateRangeMapper} represented by this.
     */
    @Override
    public DateRangeMapper build(String name) {
        return new DateRangeMapper(name, start, stop, pattern);
    }
}
