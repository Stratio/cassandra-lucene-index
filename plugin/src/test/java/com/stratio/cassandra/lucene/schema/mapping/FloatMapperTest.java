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
import com.stratio.cassandra.lucene.schema.mapping.builder.FloatMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static org.junit.Assert.*;

public class FloatMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        FloatMapper mapper = new FloatMapperBuilder().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Boost is not set to default value", DoubleMapper.DEFAULT_BOOST, mapper.boost, 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        FloatMapper mapper = new FloatMapperBuilder().indexed(false)
                                                     .sorted(true)
                                                     .column("column")
                                                     .boost(0.3f)
                                                     .build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertFalse("Indexed is not properly set", mapper.indexed);
        assertTrue("Sorted is not properly set", mapper.sorted);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Boost is not properly set", 0.3f, mapper.boost, 1);
    }

    @Test
    public void testJsonSerialization() {
        FloatMapperBuilder builder = new FloatMapperBuilder().indexed(false).sorted(true).column("column").boost(0.3f);
        testJson(builder, "{type:\"float\",indexed:false,sorted:true,column:\"column\",boost:0.3}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        FloatMapperBuilder builder = new FloatMapperBuilder();
        testJson(builder, "{type:\"float\"}");
    }

    @Test()
    public void testSortField() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 2.3f);
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is not created", sortField);
        assertTrue("Sort field reverse is wrong", sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", null);
        assertNull("Base for nulls is wrong", parsed);
    }

    @Test()
    public void testValueString() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", "3.4");
        assertEquals("Base for strings is wrong", Float.valueOf(3.4f), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3);
        assertEquals("Base for integers is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3l);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueByte() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Byte bite= new Byte("3");
        Float parsed = mapper.base("test", bite);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueShort() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Short shorty= new Short("3");
        Float parsed = mapper.base("test", shorty);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3.5d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", 3.6d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", Float.valueOf(3.2f), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        Float parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testIndexedField() {
        FloatMapper mapper = new FloatMapper("field", null, true, true, 1f);
        Field field = mapper.indexedField("name", 3.2f);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", 3.2f, field.numericValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        FloatMapper mapper = new FloatMapper("field", null, true, true, 1f);
        Field field = mapper.sortedField("name", 3.2f);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        FloatMapper mapper = new FloatMapper("field", null, null, null, 1f);
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        FloatMapper mapper = new FloatMapper("field", null, false, false, 0.3f);
        assertEquals("Method #toString is wrong",
                     "FloatMapper{field=field, indexed=false, sorted=false, column=field, boost=0.3}",
                     mapper.toString());
    }
}
