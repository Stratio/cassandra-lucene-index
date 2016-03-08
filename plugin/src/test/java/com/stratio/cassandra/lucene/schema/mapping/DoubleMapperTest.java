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
import com.stratio.cassandra.lucene.schema.mapping.builder.DoubleMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.doubleMapper;
import static org.junit.Assert.*;

public class DoubleMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        DoubleMapper mapper = doubleMapper().build("field");
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
        DoubleMapper mapper = doubleMapper().indexed(false).sorted(true).column("column").boost(0.3f).build("field");
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
        DoubleMapperBuilder builder = doubleMapper().indexed(false).sorted(true).column("column").boost(0.3f);
        testJson(builder, "{type:\"double\",indexed:false,sorted:true,column:\"column\",boost:0.3}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DoubleMapperBuilder builder = doubleMapper();
        testJson(builder, "{type:\"double\"}");
    }

    @Test
    public void testSortField() {
        DoubleMapper mapper = doubleMapper().boost(2.3f).build("field");
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is not created", sortField);
        assertTrue("Sort field reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testValueNull() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        assertNull("Base for nulls is wrong", mapper.base("test", null));
    }

    @Test
    public void testValueString() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", "3.4");
        assertEquals("Base for strings is wrong", Double.valueOf(3.4), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3);
        assertEquals("Base for integer is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3l);
        assertEquals("Base for longs is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueByte() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Byte bite = new Byte("3");
        Double parsed = mapper.base("test", bite);
        assertEquals("Base for longs is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueShort() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Short shorty = new Short("3");
        Double parsed = mapper.base("test", shorty);
        assertEquals("Base for longs is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", Double.valueOf(3.6f), parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3d);
        assertEquals("Base for doubles is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3.5d);
        assertEquals("Base for doubles is wrong", Double.valueOf(3.5d), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", 3.6d);
        assertEquals("Base for doubles is wrong", Double.valueOf(3.6d), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", Double.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", Double.valueOf(3.2d), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        Double parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", Double.valueOf(3.6d), parsed);
    }

    @Test
    public void testIndexedField() {
        DoubleMapper mapper = doubleMapper().indexed(true).boost(1f).build("field");
        Field field = mapper.indexedField("name", 3.2d);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", 3.2d, field.numericValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        DoubleMapper mapper = doubleMapper().sorted(true).boost(1f).build("field");
        Field field = mapper.sortedField("name", 3.2d);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DoubleMapper mapper = doubleMapper().boost(1f).build("field");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        DoubleMapper mapper = doubleMapper().indexed(false).sorted(true).validated(true).boost(1f).build("field");
        assertEquals("Method #toString is wrong",
                     "DoubleMapper{field=field, indexed=false, sorted=true, validated=true, column=field, boost=1.0}",
                     mapper.toString());
    }
}
