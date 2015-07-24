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

import static org.junit.Assert.*;

public class DoubleMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(DoubleMapper.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DoubleMapper mapper = new DoubleMapper("field", false, true, 2.3f);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testSortField() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 2.3f);
        SortField sortField = mapper.sortField("field", true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test()
    public void testValueString() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.4");
        assertEquals(Double.valueOf(3.4), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3l);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3f);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.5f);
        assertEquals(Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.6f);
        assertEquals(Double.valueOf(3.6f), parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3d);
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.5d);
        assertEquals(Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", 3.6d);
        assertEquals(Double.valueOf(3.6d), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", "3");
        assertEquals(Double.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.2");
        assertEquals(Double.valueOf(3.2d), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        Double parsed = mapper.base("test", "3.6");
        assertEquals(Double.valueOf(3.6d), parsed);
    }

    @Test
    public void testIndexedField() {
        DoubleMapper mapper = new DoubleMapper("field", true, true, 1f);
        Field field = mapper.indexedField("name", 3.2d);
        assertNotNull(field);
        assertEquals(3.2d, field.numericValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        DoubleMapper mapper = new DoubleMapper("field", true, true, 1f);
        Field field = mapper.sortedField("name", 3.2d);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DoubleMapper mapper = new DoubleMapper("field", null, null, 1f);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        DoubleMapper mapper = new DoubleMapper("field", false, false, 0.3f);
        assertEquals("DoubleMapper{indexed=false, sorted=false, boost=0.3}", mapper.toString());
    }
}
