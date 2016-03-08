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

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.StringMapperBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

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
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("field"));
        assertEquals("Case sensitive is not set to default value",
                     StringMapper.DEFAULT_CASE_SENSITIVE,
                     mapper.caseSensitive);
    }

    @Test
    public void testConstructorWithAllArgs() {
        StringMapper mapper = stringMapper().indexed(false)
                                            .sorted(true)
                                            .column("column")
                                            .caseSensitive(false)
                                            .build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertFalse("Indexed is not set", mapper.indexed);
        assertTrue("Sorted is not set", mapper.sorted);
        assertEquals("Column is not set", "column", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("column"));
        assertFalse("Case sensitive is not set", mapper.caseSensitive);
    }

    @Test
    public void testJsonSerialization() {
        StringMapperBuilder builder = stringMapper().indexed(false).sorted(true).column("column").caseSensitive(false);
        testJson(builder, "{type:\"string\",indexed:false,sorted:true,column:\"column\",case_sensitive:false}");
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
        String parsed = mapper.base("test", 3l);
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
        Field field = mapper.indexedField("name", "hello");
        assertNotNull("Indexed field name is not created", field);
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field value is wrong", "hello", field.stringValue());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        Field field = mapper.sortedField("name", "hello");
        assertNotNull("Sorted field name is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
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
        StringMapper mapper = stringMapper().sorted(true).caseSensitive(true).build("field");
        Document document = new Document();
        Column<?> column = Column.builder("field").buildWithComposed("value", UTF8Type.instance);
        Columns columns = new Columns(column);
        mapper.addFields(document, columns);
        IndexableField[] indexableFields = document.getFields("field");
        assertEquals("Number of created fields is wrong", 2, indexableFields.length);
        assertTrue("Indexed field is not properly created", indexableFields[0] instanceof Field);
        assertEquals("Indexed field type is wrong", KeywordMapper.FIELD_TYPE, indexableFields[0].fieldType());
        assertTrue("Sorted field is not properly created", indexableFields[1] instanceof SortedDocValuesField);
    }

    @Test
    public void testExtractAnalyzers() {
        StringMapper mapper = stringMapper().caseSensitive(true).build("field");
        String analyzer = mapper.analyzer;
        assertEquals("Method #getAnalyzer is wrong", Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        StringMapper mapper = stringMapper().indexed(false)
                                            .sorted(true)
                                            .validated(true)
                                            .caseSensitive(true)
                                            .build("field");
        assertEquals("Method #toString is wrong",
                     "StringMapper{field=field, indexed=false, sorted=true, validated=true, column=field, " +
                     "caseSensitive=true}",
                     mapper.toString());
    }
}
