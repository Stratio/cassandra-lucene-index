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

import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link SingleColumnMapperBuilder} to build a new {@link TextMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapperBuilder extends SingleColumnMapperBuilder<TextMapper, TextMapperBuilder> {

    @JsonProperty("analyzer")
    private String analyzer;

    /**
     * Sets the name of the {@link org.apache.lucene.analysis.Analyzer} to be used.
     *
     * @param analyzer the name of the {@link org.apache.lucene.analysis.Analyzer} to be used
     * @return this
     */
    public TextMapperBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Returns the {@link TextMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link TextMapper} represented by this
     */
    @Override
    public TextMapper build(String field) {
        return new TextMapper(field, column, indexed, sorted, validated, analyzer);
    }
}
