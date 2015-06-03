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

public class ColumnMapperDoubleTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, null);
        assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(ColumnMapperDouble.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", false, true, 2.3f);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testSortField() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 2.3f);
        SortField sortField = mapper.sortField(true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test()
    public void testValueString() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.4");
        assertEquals(Double.valueOf(3.4), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3l);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3f);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.5f);
        assertEquals(Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.6f);
        assertEquals(Double.valueOf(3.6f), parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3d);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.5d);
        assertEquals(Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.6d);
        assertEquals(Double.valueOf(3.6d), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", "3");
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.2");
        assertEquals(Double.valueOf(3.2d), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.6");
        assertEquals(Double.valueOf(3.6d), parsed);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", true, true, 1f);
        Field field = mapper.indexedField("name", 3.2d);
        assertNotNull(field);
        assertEquals(3.2d, field.numericValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", true, true, 1f);
        Field field = mapper.sortedField("name", 3.2d, false);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", true, true, 1f);
        Field field = mapper.sortedField("name", 3.2d, true);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", null, null, 1f);
        String analyzer = mapper.getAnalyzer();
        assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"double\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperDouble.class, columnMapper.getClass());
        assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        assertEquals(ColumnMapperDouble.DEFAULT_BOOST, ((ColumnMapperDouble) columnMapper).getBoost(), 1);
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"double\", indexed:\"false\", sorted:\"true\", boost:\"5\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperDouble.class, columnMapper.getClass());
        assertFalse(columnMapper.isIndexed());
        assertTrue(columnMapper.isSorted());
        assertEquals(5, ((ColumnMapperDouble) columnMapper).getBoost(), 1);
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
        ColumnMapperDouble mapper = new ColumnMapperDouble("field", false, false, 0.3f);
        assertEquals("ColumnMapperDouble{indexed=false, sorted=false, boost=0.3}", mapper.toString());
    }
}
