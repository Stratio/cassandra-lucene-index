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

package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class DateParserTest {

    @Test
    public void testParseNullDate() {
        DateParser dateParser = new DateParser("yyyy/MM/dd");
        Date date = null;
        Date dateOut = dateParser.parse(date);
        assertEquals("Date null parsed by DateParser(\"yyyy/MM/dd\") must return null", null, dateOut);
    }

    @Test
    public void testParseNullObject() {
        DateParser dateParser = new DateParser("yyyy/MM/dd");
        Object date = null;
        Date dateOut = dateParser.parse(date);
        assertEquals("Object null parsed by DateParser(\"yyyy/MM/dd\") must return null", null, dateOut);
    }

    @Test
    public void testParseNullLong() {
        DateParser dateParser = new DateParser("yyyy/MM/dd");
        Long date = null;
        Date dateOut = dateParser.parse(date);
        assertEquals("Long null parsed by DateParser(\"yyyy/MM/dd\") must return null", null, dateOut);
    }

    @Test
    public void testParseValidDate() throws ParseException {
        String pattern = "yyyy/MM/dd";
        DateParser dateParser = new DateParser(pattern);

        String dateString = "2015/11/03";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        Date date = simpleDateFormat.parse(dateString);

        Date dateOut = dateParser.parse(date);
        assertEquals("Date parsed by DateParser(\"yyyy/MM/dd\") wrong parsed",
                     dateString,
                     simpleDateFormat.format(dateOut));

    }

    @Test
    public void testParseValidObject() {

        String pattern = "yyyy/MM/dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        DateParser dateParser = new DateParser(pattern);

        String dateString = "2015/11/03";
        Date dateOut = dateParser.parse(dateString);

        assertEquals("String parsed by DateParser(\"yyyy/MM/dd\") wrong parsed",
                     dateString,
                     simpleDateFormat.format(dateOut));
    }

    @Test
    public void testParseValidLong() {

        DateParser dateParser = new DateParser(null);
        Long dateLong = 20151103l;
        Date dateOut = dateParser.parse(dateLong);

        assertEquals("Long parsed by DateParser(\"yyyy/MM/dd\") wrong parsed",
                     dateLong.toString(),
                     Long.toString(dateOut.getTime()));
    }

    @Test(expected = IndexException.class)
    public void testParseInvalidObject() throws ParseException {
        String pattern = "yyyyMMdd";
        DateParser dateParser = new DateParser(pattern);

        String dateString = "2015/11/03";

        dateParser.parse(dateString);
        fail("DateParser(" + pattern + ").parse(" + dateString + ") Must generate IndexException and does not do it");

    }

    @Test(expected = IndexException.class)
    public void testParseInvalidNegativeLong() {
        String pattern = "yyyyMMdd";
        DateParser dateParser = new DateParser(pattern);

        Long dateLong = -20152345l;//invalid long not parseable
        dateParser.parse(dateLong);
        fail("DateParser(" +
             pattern +
             ").parse(" +
             dateLong.toString() +
             ") Must generate IndexException and does not do it");

    }

    @Test
    public void testValidTimestampString() {
        String pattern = "timestamp";
        DateParser dateParser = new DateParser(pattern);
        String dateString = "2635421542648178234";
        Date date = dateParser.parse(dateString);

        Date dateToCompare = new Date(Long.parseLong(dateString));
        assertEquals("DateParser(" + pattern + ").parse(" + dateString + ") wrong parsed.", dateToCompare, date);
    }

    @Test
    public void testValidTimestampLong() {
        String pattern = "timestamp";
        DateParser dateParser = new DateParser(pattern);
        Long dateLong = 2635421542648178234l;
        Date date = dateParser.parse(dateLong);

        Date dateToCompare = new Date(dateLong);
        assertEquals(String.format("DateParser(%s).parse(%s) wrong parsed.", pattern, dateLong), dateToCompare, date);
    }

    @Test(expected = IndexException.class)
    public void testInvalidTimestampString() {
        String pattern = "timestamp";
        DateParser dateParser = new DateParser(pattern);

        String dateString = "2015/03/02";
        dateParser.parse(dateString);
        fail(String.format("DateParser(%s).parse(%s) Must generate IndexException and does not do it",
                           pattern,
                           dateString));
    }

}

