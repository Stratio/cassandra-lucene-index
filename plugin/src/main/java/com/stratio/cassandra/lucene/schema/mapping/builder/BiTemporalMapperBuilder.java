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

import com.stratio.cassandra.lucene.schema.mapping.BiTemporalMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link BiTemporalMapperBuilder}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalMapperBuilder extends MapperBuilder<BiTemporalMapper> {

    /** The name of the column containing the vtStart **/
    private final String vt_from;

    /** The name of the column containing the vtEnd **/
    private final String vt_to;

    /** The name of the column containing the ttStart **/
    private final String tt_from;

    /** The name of the column containing the ttEnd **/
    private final String tt_to;

    /** pattern of DateTime **/
    private final String pattern;

    @JsonCreator
    public BiTemporalMapperBuilder(@JsonProperty("vt_from") String vt_from,
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
    public BiTemporalMapper build(String name) {
        return new BiTemporalMapper(name, vt_from, vt_to, tt_from, tt_to, pattern);
    }
}
