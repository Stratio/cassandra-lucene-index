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

import static org.junit.Assert.*;

public class ColumnMapperIntegerTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, null);
        assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(ColumnMapperDouble.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(false, true, 2.3f);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testSortField() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 2.3f);
        mapper.init("field");
        SortField sortField = mapper.sortField(true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test()
    public void testValueString() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", "2.7");
        assertEquals(Integer.valueOf(2), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3l);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3f);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3.5f);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3.6f);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3d);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3.5d);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", 3.6d);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", "3");
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", "3.2");
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.base("test", "3.2");
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testIndexedField() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(true, true, 1f);
        Field field = mapper.indexedField("name", 3);
        assertNotNull(field);
        assertEquals(3, field.numericValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(true, true, 1f);
        Field field = mapper.sortedField("name", 3, false);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(true, true, 1f);
        Field field = mapper.sortedField("name", 3, true);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        String analyzer = mapper.getAnalyzer();
        assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"integer\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperInteger.class, columnMapper.getClass());
        assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        assertEquals(ColumnMapperInteger.DEFAULT_BOOST, ((ColumnMapperInteger) columnMapper).getBoost(), 1);
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"integer\", indexed:\"false\", sorted:\"true\", boost:\"5\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperInteger.class, columnMapper.getClass());
        assertFalse(columnMapper.isIndexed());
        assertTrue(columnMapper.isSorted());
        assertEquals(5, ((ColumnMapperInteger) columnMapper).getBoost(), 1);
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }

    @Test
    public void testToString() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(false, false, 1f);
        assertEquals("ColumnMapperInteger{indexed=false, sorted=false, boost=1.0}", mapper.toString());
    }
}
