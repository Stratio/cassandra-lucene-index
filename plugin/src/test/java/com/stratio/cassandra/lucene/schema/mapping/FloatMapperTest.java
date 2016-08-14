/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.builder.FloatMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.floatMapper;
import static org.junit.Assert.*;

public class FloatMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        FloatMapper mapper = floatMapper().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
    }

    @Test
    public void testConstructorWithAllArgs() {
        FloatMapper mapper = floatMapper().column("column").build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
    }

    @Test
    public void testJsonSerialization() {
        FloatMapperBuilder builder = floatMapper().column("column");
        testJson(builder, "{type:\"float\",column:\"column\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        FloatMapperBuilder builder = floatMapper();
        testJson(builder, "{type:\"float\"}");
    }

    @Test
    public void testSortField() {
        FloatMapper mapper = floatMapper().build("field");
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is not created", sortField);
        assertTrue("Sort field reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testValueNull() {
        FloatMapper mapper = floatMapper().build("field");
        assertNull("Base for nulls is wrong", mapper.base("test", null));
    }

    @Test
    public void testValueString() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", "3.4");
        assertEquals("Base for strings is wrong", Float.valueOf(3.4f), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        FloatMapper mapper = floatMapper().build("field");
        mapper.base("test", "error");
    }

    @Test
    public void testValueInteger() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3);
        assertEquals("Base for integers is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3l);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueByte() {
        FloatMapper mapper = floatMapper().build("field");
        Byte bite = new Byte("3");
        Float parsed = mapper.base("test", bite);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueShort() {
        FloatMapper mapper = floatMapper().build("field");
        Short shorty = new Short("3");
        Float parsed = mapper.base("test", shorty);
        assertEquals("Base for longs is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3.5d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", 3.6d);
        assertEquals("Base for doubles is wrong", Float.valueOf(3.6f), parsed);
    }

    @Test
    public void testValueStringWithoutDecimal() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", Float.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", Float.valueOf(3.2f), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        FloatMapper mapper = floatMapper().build("field");
        Float parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", Float.valueOf(3.6f), parsed);
    }

    @Test
    public void testIndexedField() {
        FloatMapper mapper = floatMapper().build("field");
        Field field = mapper.indexedField("name", 3.2f)
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field value is wrong", 3.2f, field.numericValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        FloatMapper mapper = floatMapper().build("field");
        Field field = mapper.sortedField("name", 3.2f)
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        FloatMapper mapper = floatMapper().build("field");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        FloatMapper mapper = floatMapper().validated(true).build("field");
        assertEquals("Method #toString is wrong",
                     "FloatMapper{field=field, validated=true, column=field}",
                     mapper.toString());
    }
}
