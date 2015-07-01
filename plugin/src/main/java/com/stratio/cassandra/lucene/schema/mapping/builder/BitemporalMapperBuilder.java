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

import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link BitemporalMapperBuilder}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BitemporalMapperBuilder extends MapperBuilder<BitemporalMapper> {

    /** The name of the column containing the vtStart **/
    @JsonProperty("vtFrom")
    private final String vtFrom;

    /** The name of the column containing the vtEnd **/
    @JsonProperty("vtTo")
    private final String vtTo;

    /** The name of the column containing the ttStart **/
    @JsonProperty("ttFrom")
    private final String ttFrom;

    /** The name of the column containing the ttEnd **/
    @JsonProperty("ttTo")
    private final String ttTo;

    /** Pattern of DateTime **/
    @JsonProperty("pattern")
    private String pattern;

    @JsonCreator
    public BitemporalMapperBuilder(@JsonProperty("vtFrom") String vtFrom,
                                   @JsonProperty("vtTo") String vtTo,
                                   @JsonProperty("ttFrom") String ttFrom,
                                   @JsonProperty("ttTo") String ttTo) {
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    public BitemporalMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public BitemporalMapper build(String name) {
        return new BitemporalMapper(name, vtFrom, vtTo, ttFrom, ttTo, pattern);
    }
}
