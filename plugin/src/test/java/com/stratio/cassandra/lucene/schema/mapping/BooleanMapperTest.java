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
import com.stratio.cassandra.lucene.schema.mapping.builder.BooleanMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.booleanMapper;
import static org.junit.Assert.*;

public class BooleanMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BooleanMapper mapper = booleanMapper().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
    }

    @Test
    public void testConstructorWithAllArgs() {
        BooleanMapper mapper = booleanMapper().validated(true).column("column").build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
    }

    @Test
    public void testJsonSerialization() {
        BooleanMapperBuilder builder = booleanMapper().validated(true).column("column");
        testJson(builder, "{type:\"boolean\",validated:true,column:\"column\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BooleanMapperBuilder builder = booleanMapper();
        testJson(builder, "{type:\"boolean\"}");
    }

    @Test
    public void testValueNull() {
        BooleanMapper mapper = booleanMapper().build("field");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test
    public void testValueBooleanTrue() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", true);
        assertEquals("Base value is not properly parsed", "true", parsed);
    }

    @Test
    public void testValueBooleanFalse() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", false);
        assertEquals("Base value is not properly parsed", "false", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueDate() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", new Date());
    }

    @Test(expected = IndexException.class)
    public void testValueInteger() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", 3);
    }

    @Test(expected = IndexException.class)
    public void testValueLong() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", 3L);
    }

    @Test(expected = IndexException.class)
    public void testValueFloat() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", 3.6f);
    }

    @Test(expected = IndexException.class)
    public void testValueDouble() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", 3.5d);
    }

    @Test
    public void testValueStringTrueLowercase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "true");
        assertEquals("Base value is not properly parsed", "true", parsed);
    }

    @Test
    public void testValueStringTrueUppercase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "TRUE");
        assertEquals("Base value is not properly parsed", "true", parsed);
    }

    @Test
    public void testValueStringTrueMixedCase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "TrUe");
        assertEquals("Base value is not properly parsed", "true", parsed);
    }

    @Test
    public void testValueStringFalseLowercase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "false");
        assertEquals("Base value is not properly parsed", "false", parsed);
    }

    @Test
    public void testValueStringFalseUppercase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "FALSE");
        assertEquals("Base value is not properly parsed", "false", parsed);
    }

    @Test
    public void testValueStringFalseMixedCase() {
        BooleanMapper mapper = booleanMapper().build("field");
        String parsed = mapper.base("test", "fALsE");
        assertEquals("Base value is not properly parsed", "false", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", "hello");
    }

    @Test(expected = IndexException.class)
    public void testValueUUID() {
        BooleanMapper mapper = booleanMapper().build("field");
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    public void testIndexedField() {
        BooleanMapper mapper = booleanMapper().build("field");
        Field field = mapper.indexedField("name", "true")
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field value is wrong", "true", field.stringValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BooleanMapper mapper = booleanMapper().build("field");
        Field field = mapper.sortedField("name", "true")
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BooleanMapper mapper = booleanMapper().build("field");
        assertEquals("Analyzer must be keyword", Mapper.KEYWORD_ANALYZER, mapper.analyzer);
    }

    @Test
    public void testToString() {
        BooleanMapper mapper = booleanMapper().validated(true).build("field");
        assertEquals("Method #toString is wrong",
                     "BooleanMapper{field=field, validated=true, column=field}",
                     mapper.toString());
    }
}
