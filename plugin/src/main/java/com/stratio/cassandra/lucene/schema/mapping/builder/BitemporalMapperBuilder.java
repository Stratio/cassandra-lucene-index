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
    @JsonProperty("vt_from")
    private final String vt_from;

    /** The name of the column containing the vtEnd **/
    @JsonProperty("vt_to")
    private final String vt_to;

    /** The name of the column containing the ttStart **/
    @JsonProperty("tt_from")
    private final String tt_from;

    /** The name of the column containing the ttEnd **/
    @JsonProperty("tt_to")
    private final String tt_to;

    /** Pattern of DateTime **/
    @JsonProperty("pattern")
    private final String pattern;

    @JsonCreator
    public BitemporalMapperBuilder(@JsonProperty("vt_from") String vt_from,
                                   @JsonProperty("vt_to") String vt_to,
                                   @JsonProperty("tt_from") String tt_from,
                                   @JsonProperty("tt_to") String tt_to,
                                   @JsonProperty("pattern") String pattern) {
        this.vt_from = vt_from;
        this.vt_to = vt_to;
        this.tt_from = tt_from;
        this.tt_to = tt_to;
        this.pattern = pattern;
    }

    /** {@inheritDoc} */
    @Override
    public BitemporalMapper build(String name) {
        return new BitemporalMapper(name, vt_from, vt_to, tt_from, tt_to, pattern);
    }
}
