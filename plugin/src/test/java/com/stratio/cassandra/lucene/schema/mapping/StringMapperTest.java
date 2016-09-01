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

import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.StringMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */

public class StringMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        StringMapper mapper = stringMapper().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("field"));
        assertEquals("Case sensitive is not set to default value",
                     StringMapper.DEFAULT_CASE_SENSITIVE,
                     mapper.caseSensitive);
    }

    @Test
    public void testConstructorWithAllArgs() {
        StringMapper mapper = stringMapper().validated(true).column("column").caseSensitive(false).build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not set", "column", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("column"));
        assertFalse("Case sensitive is not set", mapper.caseSensitive);
    }

    @Test
    public void testJsonSerialization() {
        StringMapperBuilder builder = stringMapper().validated(true).column("column").caseSensitive(false);
        testJson(builder, "{type:\"string\",validated:true,column:\"column\",case_sensitive:false}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        StringMapperBuilder builder = stringMapper();
        testJson(builder, "{type:\"string\"}");
    }

    @Test
    public void testValueNull() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        assertNull("Base for nulls is wrong", mapper.base("test", null));
    }

    @Test
    public void testValueInteger() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3);
        assertEquals("Base for integers is wrong", "3", parsed);
    }

    @Test
    public void testValueLong() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3L);
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueShort() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", new Short("3"));
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueByte() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", new Byte("3"));
        assertEquals("Base for longs is wrong", "3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3f);
        assertEquals("Base for floats is wrong", "3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3.5f);
        assertEquals("Base for floats is wrong", "3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3.6f);
        assertEquals("Base for floats is wrong", "3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3d);
        assertEquals("Base for double is wrong", "3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3.5d);
        assertEquals("Base for double is wrong", "3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", 3.6d);
        assertEquals("Base for double is wrong", "3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", "3");
        assertEquals("Base for strings is wrong", "3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", "3.2");
        assertEquals("Base for strings is wrong", "3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", "3.6");
        assertEquals("Base for strings is wrong", "3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("Base for UUIDs is wrong", "550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        Field field = mapper.indexedField("name", "hello")
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field value is wrong", "hello", field.stringValue());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        Field field = mapper.sortedField("name", "hello")
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testBaseCaseSensitiveDefault() {
        StringMapper mapper = stringMapper().build("field");
        String base = mapper.base("name", "Hello");
        assertEquals("Base case sensitiveness is wrong", "Hello", base);
    }

    @Test
    public void testBaseCaseSensitiveTrue() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String base = mapper.base("name", "Hello");
        assertEquals("Base case sensitiveness is wrong", "Hello", base);
    }

    @Test
    public void testBaseCaseSensitiveFalse() {
        StringMapper mapper = stringMapper().caseSensitive(false).build("field");
        String base = mapper.base("name", "Hello");
        assertEquals("Base case sensitiveness is wrong", "hello", base);
    }

    @Test
    public void testAddFields() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        Columns columns = new Columns().add("field", "value");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Number of created fields is wrong", 2, fields.size());
        assertTrue("Indexed field is not properly created", fields.get(0) instanceof Field);
        assertEquals("Indexed field type is wrong", KeywordMapper.FIELD_TYPE, fields.get(0).fieldType());
        assertTrue("Sorted field is not properly created", fields.get(1) instanceof SortedSetDocValuesField);
    }

    @Test
    public void testExtractAnalyzers() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String analyzer = mapper.analyzer;
        assertEquals("Method #analyzer is wrong", Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        StringMapper mapper = stringMapper().validated(true).caseSensitive(true).build("field");
        assertEquals("Method #toString is wrong",
                     "StringMapper{field=field, validated=true, column=field, caseSensitive=true}",
                     mapper.toString());
    }
}
