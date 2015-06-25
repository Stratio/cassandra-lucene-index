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
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;

public class BigDecimalMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, null, null);
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(BigDecimalMapper.DEFAULT_INTEGER_DIGITS, mapper.getIntegerDigits());
        assertEquals(BigDecimalMapper.DEFAULT_DECIMAL_DIGITS, mapper.getDecimalDigits());
    }

    @Test
    public void testConstructorWithAllArgs() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", false, true, 10, 5);
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(10, mapper.getIntegerDigits());
        assertEquals(5, mapper.getDecimalDigits());
    }

    @Test()
    public void testValueNull() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 10, 10);
        String parsed = mapper.base("test", null);
        assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueIntegerDigitsZero() {
        new BigDecimalMapper("field", null, null, 0, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDecimalDigitsZero() {
        new BigDecimalMapper("field", null, null, 10, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBothDigitsZero() {
        new BigDecimalMapper("field", null, null, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueIntegerDigitsNegative() {
        new BigDecimalMapper("field", null, null, -1, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDecimalDigitsNegative() {
        new BigDecimalMapper("field", null, null, 10, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBothDigitsNegative() {
        new BigDecimalMapper("field", null, null, -1, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanTrue() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 100, 100);
        mapper.base("test", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanFalse() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 100, 100);
        mapper.base("test", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 100, 100);
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 100, 100);
        mapper.base("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 100, 100);
        mapper.base("test", "0s0");
    }

    // /////////////

    @Test
    public void testValueStringMinPositive() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "1");
        assertEquals("10000.9999", parsed);
    }

    @Test
    public void testValueStringMaxPositive() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "9999.9999");
        assertEquals("19999.9998", parsed);
    }

    @Test
    public void testValueStringMinNegative() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "-1");
        assertEquals("09998.9999", parsed);
    }

    @Test
    public void testValueStringMaxNegative() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "-9999.9999");
        assertEquals("00000.0000", parsed);
    }

    @Test
    public void testValueStringZero() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "0");
        assertEquals("09999.9999", parsed);
    }

    @Test
    public void testValueStringLeadingZeros() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", "000.042");
        assertEquals("10000.0419", parsed);
    }

    // // ///

    @Test
    public void testValueIntegerMinPositive() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", 1);
        assertEquals("10000.9999", parsed);
    }

    @Test
    public void testValueIntegerMaxPositive() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", 9999.9999);
        assertEquals("19999.9998", parsed);
    }

    @Test
    public void testValueIntegerMinNegative() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", -1);
        assertEquals("09998.9999", parsed);
    }

    @Test
    public void testValueIntegerMaxNegative() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", -9999.9999);
        assertEquals("00000.0000", parsed);
    }

    @Test
    public void testValueIntegerZero() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String parsed = mapper.base("test", 0);
        assertEquals("09999.9999", parsed);
    }

    // //////

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooBigInteger() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        mapper.base("test", 10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooBigDecimal() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        mapper.base("test", 42.00001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooSmallInteger() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        mapper.base("test", -10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooSmallDecimal() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        mapper.base("test", -0.00001);
    }

    // /////

    @Test
    public void testValueIntegerNegativeMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", -99999998);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerNegativeMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", -2);
        String upper = mapper.base("test", -1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerPositiveMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", 99999998);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerPositiveMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", 1);
        String upper = mapper.base("test", 2);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerNegativeZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 0);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerPositiveZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", 0);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerExtremeSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueIntegerNegativePositiveSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 8, 100);
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalNegativeMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", -0.99999999);
        String upper = mapper.base("test", -0.99999998);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalNegativeMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", -0.2);
        String upper = mapper.base("test", -0.1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalPositiveMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", 0.99999998);
        String upper = mapper.base("test", 0.99999999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalPositiveMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", 0.1);
        String upper = mapper.base("test", 0.2);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalNegativeZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", -0.1);
        String upper = mapper.base("test", 0.0);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalPositiveZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", 0.0);
        String upper = mapper.base("test", 0.1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalExtremeSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", -0.99999999);
        String upper = mapper.base("test", 0.99999999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueDecimalNegativePositiveSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 2, 8);
        String lower = mapper.base("test", -0.1);
        String upper = mapper.base("test", 0.1);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    // ////

    @Test
    public void testValueNegativeMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -9999.9999);
        String upper = mapper.base("test", -9999.9998);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueNegativeMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -0.0002);
        String upper = mapper.base("test", -0.0001);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValuePositiveMaxSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", 9999.9998);
        String upper = mapper.base("test", 9999.9999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValuePositiveMinSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", 0.0001);
        String upper = mapper.base("test", 0.0002);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueNegativeZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -0.0001);
        String upper = mapper.base("test", 0.0);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValuePositiveZeroSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", 0.0);
        String upper = mapper.base("test", 0.0001);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueExtremeSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -9999.9999);
        String upper = mapper.base("test", 9999.9999);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueNegativePositiveSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -2.4);
        String upper = mapper.base("test", 2.4);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValuePositivePositionsSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", 1.9);
        String upper = mapper.base("test", 1.99);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testValueNegativePositionsSort() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 4, 4);
        String lower = mapper.base("test", -1.9999);
        String upper = mapper.base("test", -1.9);
        int compare = lower.compareTo(upper);
        assertTrue(compare < 0);
    }

    @Test
    public void testIndexedField() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", true, null, 4, 4);
        String base = mapper.base("name", "42.43");
        Field field = mapper.indexedField("name", base);
        assertNotNull(field);
        assertEquals("10042.4299", field.stringValue());
        assertEquals("name", field.name());
        assertFalse(field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", false, null, 4, 4);
        String base = mapper.base("name", "42.43");
        Field field = mapper.sortedField("name", base, false);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testSortedFieldCollection() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", false, null, 4, 4);
        String base = mapper.base("name", "42.43");
        Field field = mapper.sortedField("name", base, true);
        assertNotNull(field);
        assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", null, null, 10, 10);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"bigdec\"}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(BigDecimalMapper.class, mapper.getClass());
        assertEquals(Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals(Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals(BigDecimalMapper.DEFAULT_DECIMAL_DIGITS, ((BigDecimalMapper) mapper).getDecimalDigits());
        assertEquals(BigDecimalMapper.DEFAULT_INTEGER_DIGITS, ((BigDecimalMapper) mapper).getIntegerDigits());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"bigdec\", indexed:\"false\", sorted:\"true\", " +
                      "integer_digits:20, decimal_digits:30}}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNotNull(mapper);
        assertEquals(BigDecimalMapper.class, mapper.getClass());
        assertFalse(mapper.isIndexed());
        assertTrue(mapper.isSorted());
        assertEquals(20, ((BigDecimalMapper) mapper).getIntegerDigits());
        assertEquals(30, ((BigDecimalMapper) mapper).getDecimalDigits());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        Mapper mapper = schema.getMapper("age");
        assertNull(mapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }

    @Test
    public void testToString() {
        BigDecimalMapper mapper = new BigDecimalMapper("field", false, false, 8, 100);
        assertEquals("BigDecimalMapper{indexed=false, sorted=false, integerDigits=8, decimalDigits=100}",
                     mapper.toString());
    }
}
