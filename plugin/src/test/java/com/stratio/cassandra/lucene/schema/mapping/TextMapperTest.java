/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class TextMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        TextMapper mapper = new TextMapper("field", null, null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(TextMapper.DEFAULT_ANALYZER, mapper.getAnalyzer());
    }

    @Test
    public void testConstructorWithAllArgs() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals("SpanishAnalyzer", mapper.getAnalyzer());
    }

    @Test()
    public void testBaseClass() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        assertEquals(String.class, mapper.baseClass());
    }

    @Test()
    public void testSortField() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        SortField sortField = mapper.sortField(true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testAnalyzerNull() {
        TextMapper mapper = new TextMapper("field", true, true, null);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test()
    public void testValueNull() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueLong() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3l);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3f);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5f);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6f);
        assertEquals("3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3d);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5d);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6d);
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3");
        assertEquals("3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.2");
        assertEquals("3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.6");
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.indexedField("name", "hello");
        assertNotNull(field);
        assertEquals("hello", field.stringValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        TextMapper mapper = new TextMapper("field", null, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.sortedField("name", "hello", false);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        TextMapper mapper = new TextMapper("field", null, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.sortedField("name", "hello", true);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        assertEquals("org.apache.lucene.analysis.en.EnglishAnalyzer", mapper.getAnalyzer());
    }

    @Test
    public void testParseJSONWithAnalyzer() throws IOException {
        String json = "{fields:{age:{type:\"text\", analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(TextMapper.class, mapper.getClass());
    }

    @Test
    public void testParseJSONWithoutAnalyzer() throws IOException {
        String json = "{fields:{age:{type:\"text\"}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(TextMapper.class, mapper.getClass());
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"text\"}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(TextMapper.class, mapper.getClass());
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(TextMapper.DEFAULT_ANALYZER, mapper.getAnalyzer());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"text\", indexed:\"false\", sorted:\"true\", analyzer:\"spanish\"}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(TextMapper.class, mapper.getClass());
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals("spanish", mapper.getAnalyzer());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNull(mapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }

    @Test
    public void testToString() {
        TextMapper mapper = new TextMapper("field", false, false, "English");
        assertEquals("TextMapper{indexed=false, sorted=false, analyzer=English}", mapper.toString());
    }
}
