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

import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public class ColumnMapperBooleanTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(false, true);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueBooleanTrue() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", true);
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueBooleanFalse() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", false);
        Assert.assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", 3.6f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", 3.5d);
    }

    @Test
    public void testValueStringTrueLowercase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "true");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueUppercase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "TRUE");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueMixedCase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "TrUe");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringFalseLowercase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "false");
        Assert.assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseUppercase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "FALSE");
        Assert.assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseMixedCase() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String parsed = mapper.base("test", "fALsE");
        Assert.assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", "hello");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    public void testIndexedField() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(true, true);
        Field field = mapper.indexedField("name", "true");
        Assert.assertNotNull(field);
        Assert.assertNotNull(field);
        Assert.assertEquals("true", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertFalse(field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(true, false);
        Field field = mapper.sortedField("name", "true", false);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(true, false);
        Field field = mapper.sortedField("name", "true", true);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean(null, null);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"boolean\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBoolean.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"boolean\", indexed:\"false\", sorted:\"true\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBoolean.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
