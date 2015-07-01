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

public class IntegerMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(DoubleMapper.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        IntegerMapper mapper = new IntegerMapper("field", false, true, 2.3f);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testSortField() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 2.3f);
        SortField sortField = mapper.sortField(true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test()
    public void testValueString() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", "2.7");
        assertEquals(Integer.valueOf(2), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3l);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3f);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3.5f);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3.6f);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3d);
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3.5d);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", 3.6d);
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", "3");
        assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", "3.2");
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        Integer parsed = mapper.base("test", "3.2");
        assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testIndexedField() {
        IntegerMapper mapper = new IntegerMapper("field", true, true, 1f);
        Field field = mapper.indexedField("name", 3);
        assertNotNull(field);
        assertEquals(3, field.numericValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        IntegerMapper mapper = new IntegerMapper("field", true, true, 1f);
        Field field = mapper.sortedField("name", 3, false);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        IntegerMapper mapper = new IntegerMapper("field", true, true, 1f);
        Field field = mapper.sortedField("name", 3, true);
        assertNotNull(field);
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        IntegerMapper mapper = new IntegerMapper("field", null, null, 1f);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        IntegerMapper mapper = new IntegerMapper("field", false, false, 1f);
        assertEquals("IntegerMapper{indexed=false, sorted=false, boost=1.0}", mapper.toString());
    }
}
