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

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class ColumnMapperStringTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperString mapper = new ColumnMapperString(null, null, null);
        assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(ColumnMapperString.DEFAULT_CASE_SENSITIVE, mapper.isCaseSensitive());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperString mapper = new ColumnMapperString(false, true, false);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertFalse(mapper.isCaseSensitive());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3l);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3f);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3.5f);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3.6f);
        assertEquals("3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3d);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3.5d);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", 3.6d);
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", "3");
        assertEquals("3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", "3.2");
        assertEquals("3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", "3.6");
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        Field field = mapper.indexedField("name", "hello");
        assertNotNull(field);
        assertEquals("hello", field.stringValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        Field field = mapper.sortedField("name", "hello", false);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        Field field = mapper.sortedField("name", "hello", true);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testBaseCaseSensitiveDefault() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, null);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("Hello", base);
    }

    @Test
    public void testBaseCaseSensitiveTrue() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("Hello", base);
    }

    @Test
    public void testBaseaseSensitiveFalse() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, false);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("hello", base);
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperString mapper = new ColumnMapperString(true, true, true);
        String analyzer = mapper.getAnalyzer();
        assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testAddFields() {
        ColumnMapperString mapper = new ColumnMapperString(null, null, null);
        mapper.init("field");
        Document document = new Document();
        Column column = Column.fromComposed("field", "value", UTF8Type.instance, false);
        Columns columns = new Columns(column);
        mapper.addFields(document, columns);
        IndexableField[] indexableFields = document.getFields("field");
        assertEquals(2, indexableFields.length);
        assertTrue(indexableFields[0] instanceof StringField);
        assertTrue(indexableFields[1] instanceof SortedDocValuesField);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"string\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperString.class, columnMapper.getClass());
        assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        assertEquals(ColumnMapperString.DEFAULT_CASE_SENSITIVE, ((ColumnMapperString) columnMapper).isCaseSensitive());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"string\", indexed:\"false\", sorted:\"true\", case_sensitive:\"false\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNotNull(columnMapper);
        assertEquals(ColumnMapperString.class, columnMapper.getClass());
        assertFalse(columnMapper.isIndexed());
        assertTrue(columnMapper.isSorted());
        assertFalse(((ColumnMapperString) columnMapper).isCaseSensitive());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }

    @Test
    public void testToString() {
        ColumnMapperString mapper = new ColumnMapperString(false, false, false);
        assertEquals("ColumnMapperString{indexed=false, sorted=false, caseSensitive=false}", mapper.toString());
    }
}
