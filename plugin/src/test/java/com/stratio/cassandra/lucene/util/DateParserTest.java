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
import com.stratio.cassandra.lucene.column.Column;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;

import static com.stratio.cassandra.lucene.util.DateParser.TIMESTAMP_PATTERN;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class DateParserTest {

    private static void assertNull(String pattern, Object value) {
        DateParser parser = new DateParser(pattern);
        Date date = parser.parse(value);
        Assert.assertNull(String.format("%s for %s should return null", parser, value), date);
    }

    private static void assertEquals(String pattern, Column<?> column, Date expected) {
        assertEquals(pattern, pattern, column, expected);
    }

    private static void assertEquals(String columnPattern, String lucenePattern, Column<?> column, Date expected) {
        DateParser parser = new DateParser(columnPattern, lucenePattern);
        Date date = parser.parse(column);
        Assert.assertEquals(String.format("%s for %s should generate %s but get %s", parser, column, expected, date),
                            expected, date);
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
    public void testWithPatternNull() {
        assertNull("yyyy/MM/dd", null);
    }

    @Test
    public void testWithPatternDate() throws ParseException {
        Date date = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", date, date);
    }

    @Test
    public void testWithPatternMinDate() throws ParseException {
        Date date = new Date(Long.MIN_VALUE);
        assertEquals("yyyy/MM/dd", date, date);
    }

    @Test
    public void testWithPatternMaxDate() throws ParseException {
        Date date = new Date(Long.MAX_VALUE);
        assertEquals("yyyy/MM/dd", date, date);
    }

    @Test
    public void testWithPatternDateTruncating() throws ParseException {
        Date date = date("yyyy/MM/dd HH:mm:ss", "2015/11/03 01:02:03");
        Date expected = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", date, expected);
    }

    @Test
    public void testWithPatternLong() throws ParseException {
        Date date = date("yyyyMMdd", "20151103");
        assertEquals("yyyyMMdd", 20151103L, date);
    }

    @Test
    public void testWithPatternLongInvalid() throws ParseException {
        assertFail("yyyyMMdd", 1);
    }

    @Test
    public void testWithPatternLongNegative() throws ParseException {
        assertFail("yyyyMMdd", -20151103L);
    }

    @Test
    public void testWithPatternString() throws ParseException {
        Date expected = date("yyyy/MM/dd", "2015/11/03");
        assertEquals("yyyy/MM/dd", "2015/11/03", expected);
    }

    @Test
    public void testWithPatternStringInvalid() throws ParseException {
        assertFail("yyyy/MM/dd", "20151103");
    }

    @Test
    public void testWithTimestampNull() {
        assertNull(TIMESTAMP_PATTERN, null);
    }

    @Test
    public void testWithTimestampMinDate() throws ParseException {
        Date date = new Date(Long.MIN_VALUE);
        assertEquals(TIMESTAMP_PATTERN, Long.MIN_VALUE, date);
    }

    @Test
    public void testWithTimestampMaxDate() throws ParseException {
        Date date = new Date(Long.MAX_VALUE);
        assertEquals(TIMESTAMP_PATTERN, Long.MAX_VALUE, date);
    }

    @Test
    public void testWithTimestampDate() throws ParseException {
        Date date = date("yyyy/MM/dd", "2015/11/03");
        assertEquals(TIMESTAMP_PATTERN, date, date);
    }

    @Test
    public void testWithTimestampString() {
        Long timestamp = 2635421542648178234L;
        assertEquals(TIMESTAMP_PATTERN, timestamp.toString(), new Date(timestamp));
    }

    @Test
    public void testWithTimestampStringInvalid() {
        assertFail(TIMESTAMP_PATTERN, "2015/03/02");
    }

    @Test
    public void testWithTimestampLong() {
        Long timestamp = 2635421542648178234L;
        assertEquals(TIMESTAMP_PATTERN, timestamp, new Date(timestamp));
    }

    @Test
    public void testWithPatternColumnNull() {
        Column<?> column = null;
        assertNull("yyyy/MM/dd", column);
    }

    @Test
    public void testWithTimestampColumnNull() {
        Column<?> column = null;
        assertNull(TIMESTAMP_PATTERN, column);
    }

    @Test
    public void testColumnSimpleDateWithPattern() throws ParseException {
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        Column<Integer> column = Column.builder("date").buildWithDecomposed(bb, SimpleDateType.instance);
        Date expectedDate = date("yyyy-MM-dd", "2015-10-10");
        assertEquals("yyyy-MM-dd", column, expectedDate);
    }

    @Test
    public void testColumnSimpleDateWithPatternTruncating() throws ParseException {
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        Column<Integer> column = Column.builder("date").buildWithDecomposed(bb, SimpleDateType.instance);
        Date expectedDate = date("yyyy/MM", "2015/10");
        assertEquals("yyyy-MM", column, expectedDate);
    }

    @Test
    public void testColumnSimpleDateWithTimestamp() {
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        Column<Integer> column = Column.builder("date").buildWithDecomposed(bb, SimpleDateType.instance);
        Date expectedDate = new Date(1444435200000L);
        assertEquals(TIMESTAMP_PATTERN, column, expectedDate);
    }

    @Test
    public void testColumnDateWithPattern() throws ParseException {
        ByteBuffer bb = TimestampType.instance.fromString("2015-10-10");
        Column<Date> column = Column.builder("date").buildWithDecomposed(bb, TimestampType.instance);
        Date expectedDate = date("yyyy-MM-dd", "2015-10-10");
        assertEquals("yyyy-MM-dd", column, expectedDate);
    }

    @Test
    public void testColumnDateWithPatternTruncating() throws ParseException {
        ByteBuffer bb = TimestampType.instance.fromString("2015-10-10 01:02:03");
        Column<Date> column = Column.builder("date").buildWithDecomposed(bb, TimestampType.instance);
        Date expectedDate = date("yyyy-MM-dd", "2015-10-10");
        assertEquals("yyyy-MM-dd", column, expectedDate);
    }

    @Test
    public void testColumnLongWithDifferentPatterns() throws ParseException {
        Column<Long> column = Column.builder("date").buildWithComposed(20151127010203L, LongType.instance);
        Date expectedDate = date("yyyy-MM-dd", "2015-11-27");
        assertEquals("yyyyMMddHHmmss", "yyyy-MM-dd", column, expectedDate);
    }

    @Test
    public void testColumnUTF8WithDifferentPatterns() throws ParseException {
        Column<String> column = Column.builder("date").buildWithComposed("20151127010203", UTF8Type.instance);
        Date expectedDate = date("yyyy-MM-dd", "2015-11-27");
        assertEquals("yyyyMMddHHmmss", "yyyy-MM-dd", column, expectedDate);
    }

}

