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
import java.util.UUID;

public class ColumnMapperTextTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperText mapper = new ColumnMapperText(null, null, null);
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        Assert.assertEquals(ColumnMapperText.DEFAULT_ANALYZER, mapper.getAnalyzer());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperText mapper = new ColumnMapperText(false, true, "SpanishAnalyzer");
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
        Assert.assertEquals("SpanishAnalyzer", mapper.getAnalyzer());
    }

    @Test()
    public void testAnalyzerNull() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, null);
        String parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test()
    public void testValueNull() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3);
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3l);
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3f);
        Assert.assertEquals("3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5f);
        Assert.assertEquals("3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6f);
        Assert.assertEquals("3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3d);
        Assert.assertEquals("3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.5d);
        Assert.assertEquals("3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", 3.6d);
        Assert.assertEquals("3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3");
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.2");
        Assert.assertEquals("3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", "3.6");
        Assert.assertEquals("3.6", parsed);

    }

    @Test
    public void testValueUUID() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.indexedField("name", "hello");
        Assert.assertNotNull(field);
        Assert.assertEquals("hello", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperText mapper = new ColumnMapperText(null, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.sortedField("name", "hello", false);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperText mapper = new ColumnMapperText(null, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.sortedField("name", "hello", true);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperText mapper = new ColumnMapperText(true, true, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        Assert.assertEquals("org.apache.lucene.analysis.en.EnglishAnalyzer", mapper.getAnalyzer());
    }

    @Test
    public void testParseJSONWithAnalyzer() throws IOException {
        String json = "{fields:{age:{type:\"text\", analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONWithoutAnalyzer() throws IOException {
        String json = "{fields:{age:{type:\"text\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"text\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        Assert.assertEquals(ColumnMapperText.DEFAULT_ANALYZER, columnMapper.getAnalyzer());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"text\", indexed:\"false\", sorted:\"true\", analyzer:\"spanish\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
        Assert.assertEquals("spanish", columnMapper.getAnalyzer());
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
