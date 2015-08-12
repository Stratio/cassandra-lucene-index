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

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        LongMapper mapper = new LongMapper("field", null, null, null, null);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("Boost is not set to default value", DoubleMapper.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        LongMapper mapper = new LongMapper("field", null, false, true, 2.3f);
        assertFalse("Indexed is not properly set", mapper.isIndexed());
        assertTrue("Sorted is not properly set", mapper.isSorted());
        assertEquals("Boost is not properly set", 2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testSortField() {
        LongMapper mapper = new LongMapper("field", null, null, null, 2.3f);
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is not created", sortField);
        assertTrue("Sort field reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testValueNull() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", null);
        assertNull("Base for nulls is wrong", parsed);
    }

    @Test()
    public void testValueString() {
        LongMapper mapper = new LongMapper("field", null, null, null, 1f);
        Long parsed = mapper.base("test", "3");
        assertEquals("Base for string is wrong", Long.valueOf(3), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        LongMapper mapper = new LongMapper("field", null, null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3);
        assertEquals("Base for integer is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3l);
        assertEquals("Base for long is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3f);
        assertEquals("Base for float is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3.5f);
        assertEquals("Base for float is wrong", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3.6f);
        assertEquals("Base for float is wrong", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3d);
        assertEquals("Base for double is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3.5d);
        assertEquals("Base for double is wrong", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", 3.6d);
        assertEquals("Base for double is wrong", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", "3");
        assertEquals("Base for string is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", "3.2");
        assertEquals("Base for string is wrong", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", "3.2");
        assertEquals("Base for string is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testIndexedField() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Field field = mapper.indexedField("name", 3L);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", 3L, field.numericValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Field field = mapper.sortedField("name", 3L);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        assertNull("Analyzer must be null", mapper.getAnalyzer());
    }

    @Test
    public void testToString() {
        LongMapper mapper = new LongMapper("field", null, false, false, 0.3f);
        assertEquals("Method #toString is wrong",
                     "LongMapper{name=field, indexed=false, sorted=false, column=field, boost=0.3}",
                     mapper.toString());
    }
}
