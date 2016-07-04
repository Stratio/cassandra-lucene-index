/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.UUID;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class DateParserTest {

    private static void assertNull(String pattern, Object value) {
        DateParser parser = new DateParser(pattern);
        Date date = parser.parse(value);
        Assert.assertNull(String.format("%s for %s should return null", parser, value), date);
    }

    private static void assertEquals(String pattern, Object value, Date expected) {
        DateParser parser = new DateParser(pattern);
        Date date = parser.parse(value);
        Assert.assertEquals(String.format("%s for %s should generate %s but get %s", parser, value, expected, date),
                            expected, date);
    }

    private static void assertFail(String pattern, Object value) {
        DateParser parser = new DateParser(pattern);
        try {
            Date date = parser.parse(value);
            Assert.fail(String.format("%s for %s should throw an IndexException but returned %s", parser, value, date));
        } catch (IndexException e) {
            // Nothing to do here
        }
    }

    private static Date date(String format, String input) {
        return DateTimeFormat.forPattern(format).parseDateTime(input).toDate();
    }

    @Test
    public void testParseNull() {
        assertNull("yyyy/MM/dd", null);
    }

    @Test
    public void testParseDate() throws ParseException {
        Date date = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", date, date);
    }

    @Test
    public void testParseMinDate() throws ParseException {
        Date date = new Date(0);
        assertEquals(DateParser.DEFAULT_PATTERN, date, date);
    }

    @Test
    public void testParseMaxDate() throws ParseException {
        Date date = new Date(Long.MAX_VALUE);
        assertEquals(DateParser.DEFAULT_PATTERN, date, date);
    }

    @Test
    public void testParseDateTruncating() throws ParseException {
        Date date = date("yyyy/MM/dd HH:mm:ss", "2015/11/03 01:02:03");
        Date expected = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", date, expected);
    }

    @Test
    public void testParseInteger() throws ParseException {
        Date date = date("yyyyMMdd", "20151103");
        assertEquals("yyyyMMdd", 20151103, date);
    }

    @Test
    public void testParseIntegerInvalid() throws ParseException {
        assertFail("yyyyMMdd", 1);
    }

    @Test
    public void testParseIntegerNegative() throws ParseException {
        assertFail("yyyyMMdd", -20151103);
    }



    @Test
    public void testParseLong() throws ParseException {
        Date date = date("yyyyMMdd", "20151103");
        assertEquals("yyyyMMdd", 20151103L, date);
    }

    @Test
    public void testParseLongInvalid() throws ParseException {
        assertFail("yyyyMMdd", 1L);
    }

    @Test
    public void testParseLongNegative() throws ParseException {
        assertFail("yyyyMMdd", -20151103L);
    }

    @Test
    public void testParseFloat() throws ParseException {
        Date date = date("yyyy", "2015");
        assertEquals("yyyy", 2015f, date);
    }

    @Test
    public void testParseFloatWithDecimal() throws ParseException {
        Date date = date("yyyy", "2015");
        assertEquals("yyyy", 2015.7f, date);
    }

    @Test
    public void testParseFloatInvalid() throws ParseException {
        assertFail("yyMM", 1f);
    }

    @Test
    public void testParseFloatNegative() throws ParseException {
        assertFail("yyyy", -2015f);
    }

    @Test
    public void testParseDouble() throws ParseException {
        Date date = date("yyyyMM", "201511");
        assertEquals("yyyyMM", 201511d, date);
    }

    @Test
    public void testParseDoubleWithDecimal() throws ParseException {
        Date date = date("yyyyMM", "201511");
        assertEquals("yyyyMM", 201511.7d, date);
    }

    @Test
    public void testParseDoubleInvalid() throws ParseException {
        assertFail("yyyyMM", 1d);
    }

    @Test
    public void testParseDoubleNegative() throws ParseException {
        assertFail("yyyyMM", -201511d);
    }

    @Test
    public void testParseString() throws ParseException {
        Date expected = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", "2015/11/03", expected);
    }

    @Test
    public void testParseStringInvalid() throws ParseException {
        assertFail("yyyy/MM/dd", "20151103");
    }

    @Test
    public void testParseUUID() throws ParseException {
        UUID uuid = UUIDGen.getTimeUUID(date("yyyy-MM-dd HH:mm", "2015-11-03 06:23").getTime());
        Date expected = date("yyyyMMdd", "20151103");
        assertEquals("yyyyMMdd", uuid, expected);
    }

}

