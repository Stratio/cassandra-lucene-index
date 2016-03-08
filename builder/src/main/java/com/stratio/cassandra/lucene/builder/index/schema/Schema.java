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

package com.stratio.cassandra.lucene.builder.index.schema;

import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.Analyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The user-defined mapping from Cassandra columns to Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Schema extends Builder {

    /** The default analyzer. */
    @JsonProperty("default_analyzer")
    String defaultAnalyzerName;

    /** The analyzers. */
    @JsonProperty("analyzers")
    Map<String, Analyzer> analyzers;

    /** The mappers. */
    @JsonProperty("fields")
    Map<String, Mapper> mappers;

    /**
     * Sets the name of the default {@link Analyzer}.
     *
     * @param name the name of the default {@link Analyzer}
     * @return this with the specified default analyzer
     */
    public Schema defaultAnalyzer(String name) {
        defaultAnalyzerName = name;
        return this;
    }

    /**
     * Adds a new {@link Analyzer}.
     *
     * @param name the name of the {@link Analyzer} to be added
     * @param analyzer the {@link Analyzer} to be added
     * @return this with the specified analyzer
     */
    public Schema analyzer(String name, Analyzer analyzer) {
        if (analyzers == null) {
            analyzers = new LinkedHashMap<>();
        }
        analyzers.put(name, analyzer);
        return this;
    }

    /**
     * Adds a new {@link Mapper}.
     *
     * @param field the name of the {@link Mapper} to be added
     * @param mapper the {@link Mapper} to be added
     * @return this with the specified mapper
     */
    public Schema mapper(String field, Mapper mapper) {
        if (mappers == null) {
            mappers = new LinkedHashMap<>();
        }
        mappers.put(field, mapper);
        return this;
    }
}
