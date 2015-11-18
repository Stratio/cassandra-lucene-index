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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.builder.LongMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        LongMapper mapper = new LongMapperBuilder().build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("field"));
        assertEquals("Boost is not set to default value", DoubleMapper.DEFAULT_BOOST, mapper.boost, 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        LongMapper mapper = new LongMapperBuilder().indexed(false)
                                                   .sorted(true)
                                                   .column("column")
                                                   .boost(2.3f)
                                                   .build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertFalse("Indexed is not set", mapper.indexed);
        assertTrue("Sorted is not set", mapper.sorted);
        assertEquals("Column is not set", "column", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("column"));
        assertEquals("Boost is not set", 2.3f, mapper.boost, 1);
    }

    @Test
    public void testJsonSerialization() {
        LongMapperBuilder builder = new LongMapperBuilder().indexed(false).sorted(true).column("column").boost(0.3f);
        testJson(builder, "{type:\"long\",indexed:false,sorted:true,column:\"column\",boost:0.3}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        LongMapperBuilder builder = new LongMapperBuilder();
        testJson(builder, "{type:\"long\"}");
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
    public void testValueShort() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", new Short("3"));
        assertEquals("Base for long is wrong", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueByte() {
        LongMapper mapper = new LongMapper("field", null, true, true, 1f);
        Long parsed = mapper.base("test", new Byte("3"));
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
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        LongMapper mapper = new LongMapper("field", null, false, false, 0.3f);
        assertEquals("Method #toString is wrong",
                     "LongMapper{field=field, indexed=false, sorted=false, column=field, boost=0.3}",
                     mapper.toString());
    }
}
