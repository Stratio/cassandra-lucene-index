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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaAnalyzerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNull() {
        Analyzer defaultAnalyzer = new EnglishAnalyzer();
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        schemaAnalyzer.getAnalyzer(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerBlank() {
        Analyzer defaultAnalyzer = new EnglishAnalyzer();
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        schemaAnalyzer.getAnalyzer(" \t");
    }

    @Test
    public void testGetAnalyzerNotExistent() {
        Analyzer defaultAnalyzer = new EnglishAnalyzer();
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        schemaAnalyzer.getAnalyzer("failing");
    }

    @Test
    public void testStaticGetAnalyzer() {
        Analyzer expectedAnalyzer = new EnglishAnalyzer();
        Map<String, Analyzer> analyzers = new HashMap<>();
        analyzers.put("analyzer", expectedAnalyzer);
        Analyzer actualAnalyzer = SchemaAnalyzer.getAnalyzer(analyzers, "analyzer");
        assertEquals("Static get analyzer is failing", expectedAnalyzer, actualAnalyzer);
    }

    @Test(expected = IndexException.class)
    public void testStaticGetAnalyzerBlank() {
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer.getAnalyzer(analyzers, " ");
    }

    @Test(expected = IndexException.class)
    public void testStaticGetAnalyzerNotExistent() {
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer.getAnalyzer(analyzers, "unexistent");
    }

    @Test
    public void testToString() {
        Analyzer defaultAnalyzer = new EnglishAnalyzer();
        Map<String, Mapper> mappers = new HashMap<>();
        mappers.put("mapper", stringMapper().build("mapper"));
        Map<String, Analyzer> analyzers = new HashMap<>();
        SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        assertNotNull("Expected not null schema", schemaAnalyzer.toString());
    }
}
