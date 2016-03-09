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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.mapping.builder.TextMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.textMapper;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        TextMapper mapper = textMapper().build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("field"));
        assertNull("Analyzer is not set to default value", mapper.analyzer);
    }

    @Test
    public void testConstructorWithAllArgs() {
        TextMapper mapper = textMapper().indexed(false)
                                        .sorted(true)
                                        .column("column")
                                        .analyzer("spanish")
                                        .build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertFalse("Indexed is not set", mapper.indexed);
        assertTrue("Sorted is not set", mapper.sorted);
        assertEquals("Column is not set", "column", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("column"));
        assertEquals("Analyzer is not set", "spanish", mapper.analyzer);
    }

    @Test
    public void testJsonSerialization() {
        TextMapperBuilder builder = textMapper().indexed(false).sorted(true).column("column").analyzer("spanish");
        testJson(builder, "{type:\"text\",indexed:false,sorted:true,column:\"column\",analyzer:\"spanish\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        TextMapperBuilder builder = textMapper();
        testJson(builder, "{type:\"text\"}");
    }

    @Test
    public void testBaseClass() {
        TextMapper mapper = textMapper().analyzer("SpanishAnalyzer").build("field");
        assertEquals("Base class is wrong", String.class, mapper.base);
    }

    @Test
    public void testSortField() {
        TextMapper mapper = textMapper().sorted(true).analyzer("SpanishAnalyzer").build("field");
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is omitted", sortField);
        assertTrue("Sort field reverse is not set", sortField.getReverse());
    }

    @Test
    public void testValueNull() {
        TextMapper mapper = textMapper().build("field");
        assertNull("Base for nulls is wrong", mapper.base("test", null));
    }

    @Test
    public void testValueInteger() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3);
        assertEquals("Base for integers is wrong", "3", parsed);
    }

    @Test
    public void testValueLong() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3l);
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueShort() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", new Short("3"));
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueByte() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", new Byte("3"));
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", "3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", "3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", "3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3d);
        assertEquals("Base for doubles is wrong", "3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3.5d);
        assertEquals("Base for doubles is wrong", "3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", 3.6d);
        assertEquals("Base for doubles is wrong", "3.6", parsed);
    }

    @Test
    public void testValueStringWithoutDecimal() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", "3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", "3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", "3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        TextMapper mapper = textMapper().analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer").build("field");
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("Base for UUIDs is wrong", "550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        TextMapper mapper = textMapper().indexed(true)
                                        .analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer")
                                        .build("field");
        Field field = mapper.indexedField("name", "hello");
        assertNotNull("Indexed field name is not created", field);
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field value is wrong", "hello", field.stringValue());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        TextMapper mapper = textMapper().sorted(true)
                                        .analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer")
                                        .build("field");
        Field field = mapper.sortedField("name", "hello");
        assertNotNull("Sorted field name is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        TextMapper mapper = textMapper().sorted(true)
                                        .analyzer("org.apache.lucene.analysis.en.EnglishAnalyzer")
                                        .build("field");
        assertEquals("Method #getAnalyzer is wrong", "org.apache.lucene.analysis.en.EnglishAnalyzer", mapper.analyzer);
    }

    @Test
    public void testToString() {
        TextMapper mapper = textMapper().sorted(true)
                                        .indexed(false)
                                        .sorted(true)
                                        .validated(true)
                                        .analyzer("English")
                                        .build("field");
        assertEquals("Method #toString is wrong",
                     "TextMapper{field=field, indexed=false, sorted=true, validated=true, column=field, " +
                     "analyzer=English}",
                     mapper.toString());
    }
}
