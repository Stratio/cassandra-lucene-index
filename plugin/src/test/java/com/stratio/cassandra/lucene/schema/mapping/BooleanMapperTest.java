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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;

public class BooleanMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        BooleanMapper mapper = new BooleanMapper("field", false, true);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
    }

    @Test()
    public void testValueNull() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test
    public void testValueBooleanTrue() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", true);
        assertEquals("true", parsed);
    }

    @Test
    public void testValueBooleanFalse() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", false);
        assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", 3.6f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", 3.5d);
    }

    @Test
    public void testValueStringTrueLowercase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "true");
        assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueUppercase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "TRUE");
        assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueMixedCase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "TrUe");
        assertEquals("true", parsed);
    }

    @Test
    public void testValueStringFalseLowercase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "false");
        assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseUppercase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "FALSE");
        assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseMixedCase() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String parsed = mapper.base("test", "fALsE");
        assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", "hello");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    public void testIndexedField() {
        BooleanMapper mapper = new BooleanMapper("field", true, true);
        Field field = mapper.indexedField("name", "true");
        assertNotNull(field);
        assertNotNull(field);
        assertEquals("true", field.stringValue());
        assertEquals("name", field.name());
        assertFalse(field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BooleanMapper mapper = new BooleanMapper("field", true, false);
        Field field = mapper.sortedField("name", "true", false);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        BooleanMapper mapper = new BooleanMapper("field", true, false);
        Field field = mapper.sortedField("name", "true", true);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BooleanMapper mapper = new BooleanMapper("field", null, null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testToString() {
        BooleanMapper mapper = new BooleanMapper("field", false, false);
        assertEquals("BooleanMapper{indexed=false, sorted=false}", mapper.toString());
    }
}
