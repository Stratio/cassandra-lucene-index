/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaTest {

    @Test
    public void testGetDefaultAnalyzer() {
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, analyzers);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals("Expected english analyzer", EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test
    public void testGetDefaultAnalyzerNotSpecified() {
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, analyzers);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        assertEquals("Expected default analyzer", EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test
    public void testGetAnalyzerNotExistent() {
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, analyzers);
        Analyzer analyzer = schema.getAnalyzer("custom");
        assertEquals("Expected default analyzer", EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNull() {
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, analyzers);
        schema.getAnalyzer(null);
        schema.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerEmpty() {
        Map<String, Mapper> mappers = new HashMap<>();
        Map<String, Analyzer> analyzers = new HashMap<>();
        Schema schema = new Schema(new EnglishAnalyzer(), mappers, analyzers);
        schema.getAnalyzer(" \t");
        schema.close();
    }

    @Test
    public void testValidateColumns() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        Columns columns = new Columns().add("field1", "value");
        schema.validate(columns);
        schema.close();
    }

    @Test(expected = IndexException.class)
    public void testValidateColumnsFailing() {
        Schema schema = SchemaBuilders.schema().mapper("field1", integerMapper().validated(true)).build();
        Columns columns = new Columns().add("field1", "value");
        schema.validate(columns);
        schema.close();
    }

    @Test
    public void testAddFields() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        Columns columns = new Columns().add("field1", "value");
        Document document = new Document();
        schema.addFields(document, columns);
        assertNotNull("Expected true", document.getField("field1"));
        schema.close();
    }

    @Test
    public void testAddFieldsFailing() {
        Schema schema = SchemaBuilders.schema().mapper("field1", integerMapper()).build();
        Columns columns = new Columns().add("field1", "value");
        Document document = new Document();
        schema.addFields(document, columns);
        assertNull("Expected true", document.getField("field1"));
    }

    @Test
    public void testGetMapper() {
        Schema schema = SchemaBuilders.schema()
                                      .mapper("field1", stringMapper())
                                      .mapper("field2.x.y.z", stringMapper())
                                      .build();
        assertNotNull("Expected true", schema.getMapper("field1"));
        assertNotNull("Expected true", schema.getMapper("field2.x.y.z"));
        assertNull("Expected false", schema.getMapper("field2.x.y"));
        assertNull("Expected false", schema.getMapper("field2.x$a.y$b"));
        schema.close();
    }

    @Test
    public void testToString() {
        Schema schema = schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        assertNotNull("Expected not null schema", schema.toString());
        schema.close();
    }
}
