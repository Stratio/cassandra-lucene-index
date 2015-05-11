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
import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

public class ColumnMapperBigIntegerTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, null);
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, mapper.isSorted());
        Assert.assertEquals(ColumnMapperBigInteger.DEFAULT_DIGITS, mapper.getDigits());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(false, true, null);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
        Assert.assertEquals(ColumnMapperBigInteger.DEFAULT_DIGITS, mapper.getDigits());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDigitsZero() {
        new ColumnMapperBigInteger(null, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDigitsNegative() {
        new ColumnMapperBigInteger(null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanTrue() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanFalse() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 100);
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 100);
        mapper.base("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", "0s0");
    }

    @Test
    public void testValueStringMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "1");
        Assert.assertEquals("01njchs", parsed);
    }

    @Test
    public void testValueStringMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "99999999");
        Assert.assertEquals("03b2ozi", parsed);
    }

    @Test
    public void testValueStringMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "-1");
        Assert.assertEquals("01njchq", parsed);
    }

    @Test
    public void testValueStringMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "-99999999");
        Assert.assertEquals("0000000", parsed);
    }

    @Test
    public void testValueStringZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "0");
        Assert.assertEquals("01njchr", parsed);
    }

    @Test
    public void testValueStringLeadingZeros() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", "000042");
        Assert.assertEquals("01njcix", parsed);
    }

    // ///

    @Test
    public void testValueIntegerMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", 1);
        Assert.assertEquals("01njchs", parsed);
    }

    @Test
    public void testValueIntegerMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", 99999999);
        Assert.assertEquals("03b2ozi", parsed);
    }

    @Test
    public void testValueIntegerMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", -1);
        Assert.assertEquals("01njchq", parsed);
    }

    @Test
    public void testValueIntegerMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", -99999999);
        Assert.assertEquals("0000000", parsed);
    }

    @Test
    public void testValueIntegerZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String parsed = mapper.base("test", 0);
        Assert.assertEquals("01njchr", parsed);
    }

    // ///

    @Test
    public void testValueLongMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", 1L);
        Assert.assertEquals("04ldqpds", parsed);
    }

    @Test
    public void testValueLongMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", 9999999999L);
        Assert.assertEquals("096rheri", parsed);
    }

    @Test
    public void testValueLongMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", -1L);
        Assert.assertEquals("04ldqpdq", parsed);
    }

    @Test
    public void testValueLongMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", -9999999999L);
        Assert.assertEquals("00000000", parsed);
    }

    @Test
    public void testValueLongZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String parsed = mapper.base("test", 0L);
        Assert.assertEquals("04ldqpdr", parsed);
    }

    // ///

    @Test
    public void testValueBigIntegerMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 20);
        String parsed = mapper.base("test", new BigInteger("1"));
        Assert.assertEquals("00l3r41ifs0q5ts", parsed);
    }

    @Test
    public void testValueBigIntegerMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 20);
        String parsed = mapper.base("test", new BigInteger("99999999999999999999"));
        Assert.assertEquals("0167i830vk1gbni", parsed);
    }

    @Test
    public void testValueBigIntegerMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 20);
        String parsed = mapper.base("test", new BigInteger("-1"));
        Assert.assertEquals("00l3r41ifs0q5tq", parsed);
    }

    @Test
    public void testValueBigIntegerMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 20);
        String parsed = mapper.base("test", new BigInteger("-99999999999999999999"));
        Assert.assertEquals("000000000000000", parsed);
    }

    @Test
    public void testValueBigIntegerZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 20);
        String parsed = mapper.base("test", new BigInteger("0"));
        Assert.assertEquals("00l3r41ifs0q5tr", parsed);
    }

    // ///

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", 1.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", 99999999.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", -1.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", -99999999.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", 0.0f);
    }

    // ///

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMinPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", 1.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMaxPositive() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", 9999999999.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMinNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", -1.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMaxNegative() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", -9999999999.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleZero() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        mapper.base("test", 0.0d);
    }

    // /

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooBig() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", 100000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooSmall() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        mapper.base("test", -100000000);
    }

    @Test
    public void testValueNegativeMaxSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", -99999998);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueNegativeMinSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", -2);
        String upper = mapper.base("test", -1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveMaxSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", 99999998);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveMinSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", 1);
        String upper = mapper.base("test", 2);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueNegativeZeroSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 0);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveZeroSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", 0);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueExtremeSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-3, compare);
    }

    @Test
    public void testValueNegativePositiveSort() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 8);
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-2, compare);
    }

    @Test
    public void testIndexedField() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(true, null, 10);
        String base = mapper.base("name", "4243");
        Field field = mapper.indexedField("name", base);
        Assert.assertNotNull(field);
        Assert.assertNotNull(field);
        Assert.assertEquals(base, field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertFalse(field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, true, 10);
        String base = mapper.base("name", "4243");
        Field field = mapper.sortedField("name", base, false);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, true, 10);
        String base = mapper.base("name", "4243");
        Field field = mapper.sortedField("name", base, true);
        Assert.assertNotNull(field);
        Assert.assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperBigInteger mapper = new ColumnMapperBigInteger(null, null, 10);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"bigint\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapper.DEFAULT_INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.DEFAULT_SORTED, columnMapper.isSorted());
        Assert.assertEquals(ColumnMapperBigInteger.DEFAULT_DIGITS, ((ColumnMapperBigInteger) columnMapper).getDigits());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"bigint\", indexed:\"false\", sorted:\"true\", digits:20}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBigInteger.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
        Assert.assertEquals(20, ((ColumnMapperBigInteger) columnMapper).getDigits());
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
