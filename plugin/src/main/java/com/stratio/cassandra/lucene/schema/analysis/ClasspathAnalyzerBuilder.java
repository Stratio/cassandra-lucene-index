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

package com.stratio.cassandra.lucene.schema.analysis;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.analysis.Analyzer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.lang.reflect.Constructor;

/**
 * {@link AnalyzerBuilder} for building {@link Analyzer}s in classpath using its default (no args) constructor.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClasspathAnalyzerBuilder extends AnalyzerBuilder {

    @JsonProperty("class")
    private final String className;

    /**
     * Builds a new {@link AnalyzerBuilder} using the specified {@link Analyzer} full class name.
     *
     * @param className an {@link Analyzer} full qualified class name
     */
    @JsonCreator
    public ClasspathAnalyzerBuilder(@JsonProperty("class") String className) {
        this.className = className;

    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        try {
            Class<?> analyzerClass = Class.forName(className);
            Constructor<?> constructor = analyzerClass.getConstructor();
            return (Analyzer) constructor.newInstance();
        } catch (Exception e) {
            throw new IndexException(e, "Not found analyzer '%s'", className);
        }
    }
}
