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
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClasspathAnalyzerBuilderTest {

    @Test
    public void testBuild() {
        String className = "org.apache.lucene.analysis.en.EnglishAnalyzer";
        ClasspathAnalyzerBuilder builder = new ClasspathAnalyzerBuilder(className);
        Analyzer analyzer = builder.analyzer();
        assertEquals("Expected EnglishAnalyzer class", EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test(expected = IndexException.class)
    public void testBuildWithWrongClassName() {
        new ClasspathAnalyzerBuilder("abc").analyzer();
    }

    @Test
    public void testParseJSON() throws IOException {
        String json = "{type:\"classpath\", class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}";
        AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        Analyzer analyzer = builder.analyzer();
        assertEquals("Expected EnglishAnalyzer class", EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{class:\"abc\"}";
        JsonSerializer.fromString(json, AnalyzerBuilder.class);
    }
}
