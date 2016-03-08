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

package com.stratio.cassandra.lucene.schema;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.index.TokenLengthAnalyzer;
import com.stratio.cassandra.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.StandardAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Variation of {@link DelegatingAnalyzerWrapper} to be used with CQL.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaAnalyzer extends DelegatingAnalyzerWrapper {
    private final TokenLengthAnalyzer defaultAnalyzer;
    private final Map<String, TokenLengthAnalyzer> fieldAnalyzers;

    /**
     * Constructs with default analyzer and a map of analyzers to use for specific fields.
     *
     * @param defaultAnalyzer the default analyzer
     * @param analyzers the user-defined analyzers
     * @param mappers the user-defined mappers
     */
    public SchemaAnalyzer(Analyzer defaultAnalyzer, Map<String, Analyzer> analyzers, Map<String, Mapper> mappers) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.defaultAnalyzer = new TokenLengthAnalyzer(defaultAnalyzer);
        this.fieldAnalyzers = new HashMap<>();
        for (Map.Entry<String, Mapper> entry : mappers.entrySet()) {
            String name = entry.getKey();
            Mapper mapper = entry.getValue();
            String analyzerName = mapper.analyzer;
            if (analyzerName != null) {
                Analyzer analyzer = getAnalyzer(analyzers, analyzerName);
                TokenLengthAnalyzer fieldAnalyzer = new TokenLengthAnalyzer(analyzer);
                fieldAnalyzers.put(name, fieldAnalyzer);
            }
        }
    }

    /**
     * Returns the {@link Analyzer} identified by the specified name. If there is no analyzer with the specified name,
     * then it will be interpreted as a class name and it will be instantiated by reflection.
     *
     * @param analyzers the per field analyzers to be used
     * @param name the name of the {@link Analyzer} to be returned
     * @return the analyzer identified by the specified name
     */
    protected static Analyzer getAnalyzer(Map<String, Analyzer> analyzers, String name) {
        if (StringUtils.isBlank(name)) {
            throw new IndexException("Not empty analyzer name required");
        }
        Analyzer analyzer = analyzers.get(name);
        if (analyzer == null) {
            analyzer = StandardAnalyzers.get(name);
            if (analyzer == null) {
                try {
                    analyzer = (new ClasspathAnalyzerBuilder(name)).analyzer();
                } catch (Exception e) {
                    throw new IndexException(e, "Not found analyzer '%s'", name);
                }
            }
        }
        return analyzer;
    }

    /**
     * Returns the default {@link Analyzer}.
     *
     * @return the default analyzer
     */
    public TokenLengthAnalyzer getDefaultAnalyzer() {
        return defaultAnalyzer;
    }

    /**
     * Returns the {@link Analyzer} identified by the specified field name.
     *
     * @param fieldName the name of the {@link Analyzer} to be returned
     * @return the {@link Analyzer} identified by the specified field name
     */
    public TokenLengthAnalyzer getAnalyzer(String fieldName) {

        if (StringUtils.isBlank(fieldName)) {
            throw new IllegalArgumentException("Not empty analyzer name required");
        }
        String name = Column.getMapperName(fieldName);
        TokenLengthAnalyzer analyzer = fieldAnalyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        } else {
            for (Map.Entry<String, TokenLengthAnalyzer> entry : fieldAnalyzers.entrySet()) {
                if (name.startsWith(entry.getKey() + ".")) {
                    return entry.getValue();
                }
            }
            return defaultAnalyzer;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return getAnalyzer(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("defaultAnalyzer", defaultAnalyzer)
                          .add("fieldAnalyzers", fieldAnalyzers)
                          .toString();
    }
}
