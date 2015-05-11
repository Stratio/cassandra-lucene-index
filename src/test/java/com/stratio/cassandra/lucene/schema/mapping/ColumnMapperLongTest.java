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

public class ColumnMapperLongTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperLong mapper = new ColumnMapperLong(null, null, null);
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        Assert.assertEquals(ColumnMapperDouble.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperLong mapper = new ColumnMapperLong(false, true, 2.3f);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
        Assert.assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test
    public void testValueNull() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3l);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3f);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3.5f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3.6f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3d);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3.5d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", 3.6d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", "3");
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", "3.2");
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Long parsed = mapper.base("test", "3.2");
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Field field = mapper.indexedField("name", 3L);
        Assert.assertNotNull(field);
        Assert.assertEquals(3L, field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Field field = mapper.sortedField("name", 3L, false);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        Field field = mapper.sortedField("name", 3L, true);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperLong mapper = new ColumnMapperLong(true, true, 1f);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"long\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperLong.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        Assert.assertEquals(ColumnMapperLong.DEFAULT_BOOST, ((ColumnMapperLong) columnMapper).getBoost(), 1);
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"long\", indexed:\"false\", sorted:\"true\", boost:\"5\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperLong.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
        Assert.assertEquals(5, ((ColumnMapperLong) columnMapper).getBoost(), 1);
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
