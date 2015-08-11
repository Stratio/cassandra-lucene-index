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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        TextMapper mapper = new TextMapper("field", null, null, null);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertNull("Analyzer is not set to default value", mapper.getAnalyzer());
    }

    @Test
    public void testConstructorWithAllArgs() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        assertFalse("Indexed is not properly set", mapper.isIndexed());
        assertTrue("Sorted is not properly set", mapper.isSorted());
        assertEquals("Analyzer is not properly set", "SpanishAnalyzer", mapper.getAnalyzer());
    }

    @Test()
    public void testBaseClass() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        assertEquals("Base class is wrong", String.class, mapper.baseClass());
    }

    @Test()
    public void testSortField() {
        TextMapper mapper = new TextMapper("field", false, true, "SpanishAnalyzer");
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is omitted", sortField);
        assertTrue("Sort field reverse is not properly set", sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        TextMapper mapper = new TextMapper("field", true, true, null);
        String parsed = mapper.base("test", null);
        assertNull("Base for nulls is wrong", parsed);
    }

    @Test
    public void testValueInteger() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3);
        assertEquals("Base for integers is wrong", "3", parsed);
    }

    @Test
    public void testValueLong() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3l);
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", "3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", "3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", "3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3d);
        assertEquals("Base for doubles is wrong", "3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5d);
        assertEquals("Base for doubles is wrong", "3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6d);
        assertEquals("Base for doubles is wrong", "3.6", parsed);
    }

    @Test
    public void testValueStringWithoutDecimal() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", "3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", "3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", "3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("Base for UUIDs is wrong", "550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.indexedField("name", "hello");
        assertNotNull("Indexed field name is not created", field);
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field value is wrong", "hello", field.stringValue());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        TextMapper mapper = new TextMapper("field", null, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.sortedField("name", "hello");
        assertNotNull("Sorted field name is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        TextMapper mapper = new TextMapper("field", true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        assertEquals("Method #getAnalyzer is wrong",
                     "org.apache.lucene.analysis.en.EnglishAnalyzer",
                     mapper.getAnalyzer());
    }

    @Test
    public void testToString() {
        TextMapper mapper = new TextMapper("field", false, false, "English");
        assertEquals("Method #toString is wrong",
                     "TextMapper{indexed=false, sorted=false, analyzer=English}",
                     mapper.toString());
    }
}
