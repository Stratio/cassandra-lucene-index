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
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    public void testValidateMetadata() throws InvalidRequestException, ConfigurationException {

        List<ColumnDef> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field1"),
                                            UTF8Type.class.getCanonicalName()).setIndex_name("field1")
                                                                              .setIndex_type(IndexType.KEYS));

        columnDefinitions.add(new ColumnDef(ByteBufferUtil.bytes("field2"),
                                            IntegerType.class.getCanonicalName()).setIndex_name("field2")
                                                                                 .setIndex_type(IndexType.KEYS));
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setColumn_metadata(columnDefinitions)
                                 .setKeyspace("Keyspace1")
                                 .setName("Standard1");
        CFMetaData metadata = ThriftConversion.fromThrift(cfDef);

        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        schema.validate(metadata);
        schema.close();
    }

    @Test
    public void testValidateColumns() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        schema.validate(columns);
        schema.close();
    }

    @Test(expected = IndexException.class)
    public void testValidateColumnsFailing() {
        Schema schema = SchemaBuilders.schema().mapper("field1", integerMapper()).build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        schema.validate(columns);
        schema.close();
    }

    @Test
    public void testMapsTrue() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        assertTrue("Expected true", schema.maps("field1"));
        schema.close();
    }

    @Test
    public void testMapsFalse() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        assertFalse("Expected false", schema.maps("field2"));
        schema.close();
    }

    @Test
    public void testMapsAllTrue() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        assertTrue("Expected true", schema.mapsAll(columns));
        schema.close();
    }

    @Test
    public void testMapsAllFalse() {
        Schema schema = SchemaBuilders.schema()
                                      .mapper("field1", stringMapper())
                                      .mapper("field2", stringMapper())
                                      .build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        assertFalse("Expected false", schema.mapsAll(columns));
        schema.close();
    }

    @Test
    public void testAddFields() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        Document document = new Document();
        schema.addFields(document, columns);
        assertNotNull("Expected true", document.getField("field1"));
        schema.close();
    }

    @Test
    public void testAddFieldsFailing() {
        Schema schema = SchemaBuilders.schema().mapper("field1", integerMapper()).build();
        Columns columns = new Columns().add(Column.fromComposed("field1", "value", UTF8Type.instance, false));
        Document document = new Document();
        schema.addFields(document, columns);
        assertNull("Expected true", document.getField("field1"));
    }

    @Test
    public void testGetMapper() {
        Schema schema = SchemaBuilders.schema().mapper("field1", stringMapper()).build();
        assertNotNull("Expected true", schema.getMapper("field1"));
        assertNotNull("Expected true", schema.getMapper("field1.a"));
        assertNotNull("Expected true", schema.getMapper("field1.a.b"));
        schema.close();
    }

    @Test
    public void testToString() {

        Schema schema = schema().mapper("field1", stringMapper()).mapper("field2", textMapper()).build();
        assertNotNull("Expected not null schema", schema.toString());
        schema.close();
    }
}
