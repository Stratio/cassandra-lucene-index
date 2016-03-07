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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.mapping.builder.BigDecimalMapperBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bigDecimalMapper;
import static org.junit.Assert.*;

public class BigDecimalMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        BigDecimalMapper mapper = bigDecimalMapper().build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertEquals("Column is not set", "field", mapper.column);
        assertTrue("Indexed is not set", mapper.indexed);
        assertFalse("Sorted is not set", mapper.sorted);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Integer digits is not set to default value",
                     BigDecimalMapper.DEFAULT_INTEGER_DIGITS,
                     mapper.integerDigits);
        assertEquals("Decimal digits is not set to default value",
                     BigDecimalMapper.DEFAULT_DECIMAL_DIGITS,
                     mapper.decimalDigits);
    }

    @Test
    public void testConstructorWithAllArgs() {
        BigDecimalMapper mapper = bigDecimalMapper().indexed(false)
                                                    .sorted(true)
                                                    .column("column")
                                                    .integerDigits(6)
                                                    .decimalDigits(8)
                                                    .build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertFalse("Indexed is not properly set", mapper.indexed);
        assertTrue("Sorted is not properly set", mapper.sorted);
        assertEquals("Integer digits is not properly set", 6, mapper.integerDigits);
        assertEquals("Decimal digits is not properly set", 8, mapper.decimalDigits);
    }

    @Test
    public void testJsonSerialization() {
        BigDecimalMapperBuilder builder = bigDecimalMapper().indexed(false)
                                                            .sorted(true)
                                                            .column("column")
                                                            .integerDigits(6)
                                                            .decimalDigits(8);
        testJson(builder,
                 "{type:\"bigdec\",indexed:false,sorted:true,column:\"column\"," +
                 "integer_digits:6,decimal_digits:8}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BigDecimalMapperBuilder builder = bigDecimalMapper();
        testJson(builder, "{type:\"bigdec\"}");
    }

    @Test
    public void testValueNull() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(10).decimalDigits(10).build("field");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test(expected = IndexException.class)
    public void testValueIntegerDigitsZero() {
        bigDecimalMapper().integerDigits(0).decimalDigits(10).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueDecimalDigitsZero() {
        bigDecimalMapper().integerDigits(10).decimalDigits(0).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueBothDigitsZero() {
        bigDecimalMapper().integerDigits(0).decimalDigits(0).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueIntegerDigitsNegative() {
        bigDecimalMapper().integerDigits(-1).decimalDigits(10).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueDecimalDigitsNegative() {
        bigDecimalMapper().integerDigits(10).decimalDigits(-1).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueBothDigitsNegative() {
        bigDecimalMapper().integerDigits(-1).decimalDigits(-1).build("field");
    }

    @Test(expected = IndexException.class)
    public void testValueBooleanTrue() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(100).decimalDigits(100).build("field");
        mapper.base("test", true);
    }

    @Test(expected = IndexException.class)
    public void testValueBooleanFalse() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(100).decimalDigits(100).build("field");
        mapper.base("test", false);
    }

    @Test(expected = IndexException.class)
    public void testValueUUID() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(100).decimalDigits(100).build("field");
        mapper.base("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test(expected = IndexException.class)
    public void testValueDate() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(100).decimalDigits(100).build("field");
        mapper.base("test", new Date());
    }

    @Test(expected = IndexException.class)
    public void testValueStringInvalid() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(100).decimalDigits(100).build("field");
        mapper.base("test", "0s0");
    }

    @Test
    public void testValueStringMinPositive() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "1");
        assertEquals("Base value is not properly parsed", "10000.9999", parsed);
    }

    @Test
    public void testValueStringMaxPositive() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "9999.9999");
        assertEquals("Base value is not properly parsed", "19999.9998", parsed);
    }

    @Test
    public void testValueStringMinNegative() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "-1");
        assertEquals("Base value is not properly parsed", "09998.9999", parsed);
    }

    @Test
    public void testValueStringMaxNegative() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "-9999.9999");
        assertEquals("Base value is not properly parsed", "00000.0000", parsed);
    }

    @Test
    public void testValueStringZero() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "0");
        assertEquals("Base value is not properly parsed", "09999.9999", parsed);
    }

    @Test
    public void testValueStringLeadingZeros() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", "000.042");
        assertEquals("Base value is not properly parsed", "10000.0419", parsed);
    }

    @Test
    public void testValueIntegerMinPositive() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", 1);
        assertEquals("Base value is not properly parsed", "10000.9999", parsed);
    }

    @Test
    public void testValueIntegerMaxPositive() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", 9999.9999);
        assertEquals("Base value is not properly parsed", "19999.9998", parsed);
    }

    @Test
    public void testValueIntegerMinNegative() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", -1);
        assertEquals("Base value is not properly parsed", "09998.9999", parsed);
    }

    @Test
    public void testValueIntegerMaxNegative() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", -9999.9999);
        assertEquals("Base value is not properly parsed", "00000.0000", parsed);
    }

    @Test
    public void testValueIntegerZero() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String parsed = mapper.base("test", 0);
        assertEquals("Base value is not properly parsed", "09999.9999", parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueTooBigInteger() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        mapper.base("test", 40002.01);
    }

    @Test(expected = IndexException.class)
    public void testValueTooBigDecimal() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        mapper.base("test", 42.00001);
    }

    @Test(expected = IndexException.class)
    public void testValueTooSmallInteger() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        mapper.base("test", -10000);
    }

    @Test(expected = IndexException.class)
    public void testValueTooSmallDecimal() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        mapper.base("test", -0.00001);
    }

    @Test
    public void testZeroValueByte() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Byte bite = new Byte("0");
        String parsed = mapper.base("test", bite);
        assertEquals("Base value is not properly parsed", "09999.9999", parsed);
    }

    @Test
    public void testMinValueByte() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Byte bite = new Byte("-128");
        String parsed = mapper.base("test", bite);
        assertEquals("Base value is not properly parsed", "09871.9999", parsed);
    }

    @Test
    public void testMaxValueByte() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Byte bite = new Byte("127");
        String parsed = mapper.base("test", bite);
        assertEquals("Base value is not properly parsed", "10126.9999", parsed);
    }

    @Test
    public void testZeroValueShort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Short shorty = new Short("0");
        String parsed = mapper.base("test", shorty);
        assertEquals("Base value is not properly parsed", "09999.9999", parsed);
    }

    @Test
    public void testMinValueShort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(4).build("field");
        Short shorty = new Short("-32768");
        String parsed = mapper.base("test", shorty);
        assertEquals("Base value is not properly parsed", "099967231.9999", parsed);

    }

    @Test
    public void testMaxValueShort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(4).build("field");
        Short shorty = new Short("32767");
        String parsed = mapper.base("test", shorty);
        assertEquals("Base value is not properly parsed", "100032766.9999", parsed);

    }

    @Test
    public void testZeroValueInt() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Integer integer = 0;
        String parsed = mapper.base("test", integer);
        assertEquals("Base value is not properly parsed", "09999.9999", parsed);

    }

    @Test
    public void testOneValueInt() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Integer integer = 1;
        String parsed = mapper.base("test", integer);
        assertEquals("Base value is not properly parsed", "10000.9999", parsed);

    }

    @Test
    public void testNegativeValueInt() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        Integer integer = -15;
        String parsed = mapper.base("test", integer);
        assertEquals("Base value is not properly parsed", "09984.9999", parsed);

    }

    @Test
    public void testValueIntegerNegativeMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", -99999998);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerNegativeMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", -2);
        String upper = mapper.base("test", -1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerPositiveMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", 99999998);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerPositiveMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", 1);
        String upper = mapper.base("test", 2);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerNegativeZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 0);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerPositiveZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", 0);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerExtremeSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", -99999999);
        String upper = mapper.base("test", 99999999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueIntegerNegativePositiveSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        String lower = mapper.base("test", -1);
        String upper = mapper.base("test", 1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalNegativeMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", -0.99999999);
        String upper = mapper.base("test", -0.99999998);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalNegativeMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", -0.2);
        String upper = mapper.base("test", -0.1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalPositiveMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", 0.99999998);
        String upper = mapper.base("test", 0.99999999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalPositiveMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", 0.1);
        String upper = mapper.base("test", 0.2);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalNegativeZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", -0.1);
        String upper = mapper.base("test", 0.0);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalPositiveZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", 0.0);
        String upper = mapper.base("test", 0.1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalExtremeSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", -0.99999999);
        String upper = mapper.base("test", 0.99999999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueDecimalNegativePositiveSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(2).decimalDigits(8).build("field");
        String lower = mapper.base("test", -0.1);
        String upper = mapper.base("test", 0.1);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueNegativeMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -9999.9999);
        String upper = mapper.base("test", -9999.9998);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueNegativeMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -0.0002);
        String upper = mapper.base("test", -0.0001);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValuePositiveMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", 9999.9998);
        String upper = mapper.base("test", 9999.9999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValuePositiveMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", 0.0001);
        String upper = mapper.base("test", 0.0002);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueNegativeZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -0.0001);
        String upper = mapper.base("test", 0.0);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValuePositiveZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", 0.0);
        String upper = mapper.base("test", 0.0001);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueExtremeSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -9999.9999);
        String upper = mapper.base("test", 9999.9999);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueNegativePositiveSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -2.4);
        String upper = mapper.base("test", 2.4);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValuePositivePositionsSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", 1.9);
        String upper = mapper.base("test", 1.99);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueNegativePositionsSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(4).decimalDigits(4).build("field");
        String lower = mapper.base("test", -1.9999);
        String upper = mapper.base("test", -1.9);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueByteNegativeMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("-128");
        Byte upperB = new Byte("-127");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueByteNegativeMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("-2");
        Byte upperB = new Byte("-1");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueBytePositiveMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("126");
        Byte upperB = new Byte("127");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueBytePositiveMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("1");
        Byte upperB = new Byte("2");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueByteNegativeZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("-1");
        Byte upperB = new Byte("0");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueBytePositiveZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("0");
        Byte upperB = new Byte("1");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueByteExtremeSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("-128");
        Byte upperB = new Byte("127");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueByteNegativePositiveSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Byte lowerB = new Byte("-1");
        Byte upperB = new Byte("1");
        String lower = mapper.base("test", lowerB);
        String upper = mapper.base("test", upperB);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortNegativeMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("-32768");
        Short upperS = new Short("-32767");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortNegativeMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("-2");
        Short upperS = new Short("-1");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortPositiveMaxSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("32766");
        Short upperS = new Short("32767");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortPositiveMinSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("1");
        Short upperS = new Short("2");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortNegativeZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("-1");
        Short upperS = new Short("0");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortPositiveZeroSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("0");
        Short upperS = new Short("1");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortExtremeSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("-32768");
        Short upperS = new Short("32767");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testValueShortNegativePositiveSort() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        Short lowerS = new Short("-1");
        Short upperS = new Short("1");
        String lower = mapper.base("test", lowerS);
        String upper = mapper.base("test", upperS);
        int compare = lower.compareTo(upper);
        assertTrue("Cassandra ordering is not preserved", compare < 0);
    }

    @Test
    public void testIndexedField() {
        BigDecimalMapper mapper = bigDecimalMapper().indexed(true).integerDigits(4).decimalDigits(4).build("field");
        String base = mapper.base("name", "42.43");
        Field field = mapper.indexedField("name", base);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", "10042.4299", field.stringValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        BigDecimalMapper mapper = bigDecimalMapper().sorted(true).integerDigits(4).decimalDigits(4).build("field");
        String base = mapper.base("name", "42.43");
        Field field = mapper.sortedField("name", base);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        assertEquals("Analyzer must be keyword", Mapper.KEYWORD_ANALYZER, mapper.analyzer);
    }

    @Test
    public void testToString() {
        BigDecimalMapper mapper = bigDecimalMapper().integerDigits(8).decimalDigits(100).build("field");
        assertEquals("Method #toString is wrong",
                     "BigDecimalMapper{field=field, indexed=true, sorted=false, validated=false, column=field, " +
                     "integerDigits=8, decimalDigits=100}",
                     mapper.toString());
    }
}
