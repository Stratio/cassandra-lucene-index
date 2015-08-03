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

import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

public class StringMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        StringMapper mapper = new StringMapper("field", null, null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(StringMapper.DEFAULT_CASE_SENSITIVE, mapper.isCaseSensitive());
    }

    @Test
    public void testConstructorWithAllArgs() {
        StringMapper mapper = new StringMapper("field", false, true, false);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertFalse(mapper.isCaseSensitive());
    }

    @Test()
    public void testValueNull() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueLong() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3l);
        assertEquals("3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3f);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3.5f);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3.6f);
        assertEquals("3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3d);
        assertEquals("3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3.5d);
        assertEquals("3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", 3.6d);
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", "3");
        assertEquals("3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", "3.2");
        assertEquals("3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", "3.6");
        assertEquals("3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        Field field = mapper.indexedField("name", "hello");
        assertNotNull(field);
        assertEquals("hello", field.stringValue());
        assertEquals("name", field.name());
        assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        Field field = mapper.sortedField("name", "hello");
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testBaseCaseSensitiveDefault() {
        StringMapper mapper = new StringMapper("field", true, true, null);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("Hello", base);
    }

    @Test
    public void testBaseCaseSensitiveTrue() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("Hello", base);
    }

    @Test
    public void testBaseaseSensitiveFalse() {
        StringMapper mapper = new StringMapper("field", true, true, false);
        String base = mapper.base("name", "Hello");
        assertNotNull(base);
        assertEquals("hello", base);
    }

    @Test
    public void testExtractAnalyzers() {
        StringMapper mapper = new StringMapper("field", true, true, true);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testAddFields() {
        StringMapper mapper = new StringMapper("field", true, true, null);
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
    public void testToString() {
        StringMapper mapper = new StringMapper("field", false, false, false);
        assertEquals("StringMapper{indexed=false, sorted=false, caseSensitive=false}", mapper.toString());
    }
}
