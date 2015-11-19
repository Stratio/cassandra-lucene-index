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

package com.stratio.cassandra.lucene.testsAT.search;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.stratio.cassandra.lucene.builder.Builder.match;

@RunWith(JUnit4.class)
public class MatchSearchAT extends AbstractSearchAT {

    @Test
    public void matchQueryAsciiFieldTest1() {
        query(match("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void matchQueryAsciiFieldTest2() {
        query(match("ascii_1", "frase")).check(0);
    }

    @Test
    public void matchQueryAsciiFieldTest3() {
        query(match("ascii_1", "frase tipo asciii")).check(0);
    }

    @Test
    public void matchQueryAsciiFieldTest4() {
        query(match("ascii_1", "")).check(0);
    }

    @Test
    public void matchQueryAsciiFieldTest5() {
        query(match("ascii_1", "frase tipo asci")).check(0);
    }

    @Test
    public void matchQueryBigintTest2() {
        query(match("bigint_1", "1000000000000000")).check(1);
    }

    @Test
    public void matchQueryBigintTest3() {
        query(match("bigint_1", "3000000000000000")).check(3);
    }

    @Test
    public void matchQueryBigintTest4() {
        query(match("bigint_1", "10000000000000000")).check(0);
    }

    @Test
    public void matchQueryBigintTest5() {
        query(match("bigint_1", "100000000000000")).check(0);
    }

    @Test
    public void matchQueryBlobTest1() {
        query(match("blob_1", "")).check(0);
    }

    @Test
    public void matchQueryBlobTest2() {
        query(match("blob_1", "3E0A16")).check(4);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchQueryBlobTest3() {
        query(match("blob_1", "3E0A161")).check(0);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchQueryBlobTest4() {
        query(match("blob_1", "3E0A1")).check(0);
    }

    @Test
    public void matchQueryBlobTest5() {
        query(match("blob_1", "3E0A15")).check(1);
    }

    @Test(expected = DriverException.class)
    public void matchQueryBooleanTest1() {
        query(match("boolean_1", "")).check(0);
    }

    @Test
    public void matchQueryBooleanTest3() {
        query(match("boolean_1", "true")).check(4);
    }

    @Test
    public void matchQueryBooleanTest4() {
        query(match("boolean_1", "false")).check(1);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchQueryBooleanTest5() {
        query(match("boolean_1", "else")).check(0);
    }

    @Test
    public void matchQueryDecimalTest2() {
        query(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchQueryDecimalTest3() {
        query(match("decimal_1", "300000000.0")).check(0);
    }

    @Test
    public void matchQueryDecimalTest4() {
        query(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchQueryDecimalTest5() {
        query(match("decimal_1", "1000000000.0")).check(1);
    }

    @Test
    public void matchQueryDateTest1() {
        query(match("date_1", new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS").format(new Date()))).check(0);
    }

    @Test
    public void matchQueryDateTest2() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        query(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchQueryDateTest3() {
        query(match("date_1", "1970/01/01 00:00:00.000")).check(0);
    }

    @Test
    public void matchQueryDateTest4() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        query(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchQueryDoubleTest1() {
        query(match("double_1", "0")).check(0);
    }

    @Test
    public void matchQueryDoubleTest2() {
        query(match("double_1", "2.0")).check(1);
    }

    @Test
    public void matchQueryDoubleTest3() {
        query(match("double_1", "2")).check(1);
    }

    @Test
    public void matchQueryDoubleTest4() {
        query(match("double_1", "2.00")).check(1);
    }

    @Test
    public void matchQueryFloatTest1() {
        query(match("float_1", "0")).check(0);
    }

    @Test
    public void matchQueryFloatTest2() {
        query(match("float_1", "2.0")).check(1);
    }

    @Test
    public void matchQueryFloatTest3() {
        query(match("float_1", "2")).check(1);
    }

    @Test
    public void matchQueryFloatTest4() {
        query(match("float_1", "2.00")).check(1);
    }

    @Test
    public void matchQueryIntegerTest1() {
        query(match("integer_1", "-2")).check(1);
    }

    @Test
    public void matchQueryIntegerTest2() {
        query(match("integer_1", "2")).check(0);
    }

    @Test
    public void matchQueryIntegerTest3() {
        query(match("integer_1", "0")).check(0);
    }

    @Test
    public void matchQueryIntegerTest4() {
        query(match("integer_1", "-1")).check(1);
    }

    @Test
    public void matchQueryUuidTest1() {
        query(match("uuid_1", "0"));
    }

    @Test
    public void matchQueryUuidTest2() {

        query(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).check(1);
    }

    @Test
    public void matchQueryUuidTest3() {
        query(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).check(InvalidQueryException.class);
    }

    @Test
    public void matchQueryTimeuuidTest1() {
        query(match("timeuuid_1", "0")).check(InvalidQueryException.class);
    }

    @Test
    public void matchQueryTimeuuidTest2() {
        query(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).check(1);
    }

    @Test
    public void matchQueryTimeuuidTest3() {
        query(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).check(InvalidQueryException.class);
    }

    @Test
    public void matchQueryInetFieldTest1() {
        query(match("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void matchQueryInetFieldTest2() {
        query(match("inet_1", "127.0.1.1")).check(1);
    }

    @Test
    public void matchQueryInetFieldTest3() {
        query(match("inet_1", "127.1.1.")).check(InvalidQueryException.class);
    }

    @Test
    public void matchQueryInetFieldTest4() {
        query(match("inet_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void matchQueryTextFieldTest1() {
        query(match("text_1", "Frase")).check(1);
    }

    @Test
    public void matchQueryTextFieldTest2() {
        query(match("text_1", "Frase*")).check(1);
    }

    @Test
    public void matchQueryTextFieldTest3() {
        query(match("text_1", "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")).check(1);
    }

    @Test
    public void matchQueryTextFieldTest4() {
        query(match("text_1", "")).check(0);
    }

    @Test
    public void matchQueryVarcharFieldTest1() {
        query(match("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void matchQueryVarcharFieldTest2() {
        query(match("varchar_1", "frase*")).check(0);
    }

    @Test
    public void matchQueryVarcharFieldTest3() {
        query(match("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void matchQueryVarcharFieldTest4() {
        query(match("varchar_1", "")).check(0);
    }

    @Test
    public void matchQueryListFieldTest1() {
        query(match("list_1", "")).check(0);
    }

    @Test
    public void matchQueryListFieldTest2() {
        query(match("list_1", "l1")).check(2);
    }

    @Test
    public void matchQueryListFieldTest3() {
        query(match("list_1", "s1")).check(0);
    }

    @Test
    public void matchQuerySetFieldTest1() {
        query(match("set_1", "")).check(0);
    }

    @Test
    public void matchQuerySetFieldTest2() {
        query(match("set_1", "l1")).check(0);
    }

    @Test
    public void matchQuerySetFieldTest3() {
        query(match("set_1", "s1")).check(2);
    }

    @Test
    public void matchQueryMapFieldTest1() {
        query(match("map_1$k1", "")).check(0);
    }

    @Test
    public void matchQueryMapFieldTest2() {
        query(match("map_1$k1", "l1")).check(0);
    }

    @Test
    public void matchQueryMapFieldTest3() {
        query(match("map_1$k1", "k1")).check(0);
    }

    @Test
    public void matchQueryMapFieldTest4() {
        query(match("map_1$k1", "v1")).check(2);
    }

    @Test
    public void matchFilterAsciiFieldTest1() {
        filter(match("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void matchFilterAsciiFieldTest2() {
        filter(match("ascii_1", "frase")).check(0);
    }

    @Test
    public void matchFilterAsciiFieldTest3() {
        filter(match("ascii_1", "frase tipo asciii")).check(0);
    }

    @Test
    public void matchFilterAsciiFieldTest4() {
        filter(match("ascii_1", "")).check(0);
    }

    @Test
    public void matchFilterAsciiFieldTest5() {
        filter(match("ascii_1", "frase tipo asci")).check(0);
    }

    @Test
    public void matchFilterBigintTest2() {
        filter(match("bigint_1", "1000000000000000")).check(1);
    }

    @Test
    public void matchFilterBigintTest3() {
        filter(match("bigint_1", "3000000000000000")).check(3);
    }

    @Test
    public void matchFilterBigintTest4() {
        filter(match("bigint_1", "10000000000000000")).check(0);
    }

    @Test
    public void matchFilterBigintTest5() {
        filter(match("bigint_1", "100000000000000")).check(0);
    }

    @Test
    public void matchFilterBlobTest1() {
        filter(match("blob_1", "")).check(0);
    }

    @Test
    public void matchFilterBlobTest2() {
        filter(match("blob_1", "3E0A16")).check(4);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchFilterBlobTest3() {
        filter(match("blob_1", "3E0A161")).check(0);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchFilterBlobTest4() {
        filter(match("blob_1", "3E0A1")).check(0);
    }

    @Test
    public void matchFilterBlobTest5() {
        filter(match("blob_1", "3E0A15")).check(1);
    }

    @Test(expected = DriverException.class)
    public void matchFilterBooleanTest1() {
        filter(match("boolean_1", "")).check(0);
    }

    @Test
    public void matchFilterBooleanTest3() {
        filter(match("boolean_1", "true")).check(4);
    }

    @Test
    public void matchFilterBooleanTest4() {
        filter(match("boolean_1", "false")).check(1);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchFilterBooleanTest5() {
        filter(match("boolean_1", "else")).check(0);
    }

    @Test
    public void matchFilterDecimalTest2() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchFilterDecimalTest3() {
        filter(match("decimal_1", "300000000.0")).check(0);
    }

    @Test
    public void matchFilterDecimalTest4() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchFilterDecimalTest5() {
        filter(match("decimal_1", "1000000000.0")).check(1);
    }

    @Test
    public void matchFilterDateTest1() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchFilterDateTest2() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchFilterDateTest3() {
        filter(match("date_1", "1970/01/01 00:00:00.000")).check(0);
    }

    @Test
    public void matchFilterDateTest4() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchFilterDoubleTest1() {
        filter(match("double_1", "0")).check(0);
    }

    @Test
    public void matchFilterDoubleTest2() {
        filter(match("double_1", "2.0")).check(1);
    }

    @Test
    public void matchFilterDoubleTest3() {
        filter(match("double_1", "2")).check(1);
    }

    @Test
    public void matchFilterDoubleTest4() {
        filter(match("double_1", "2.00")).check(1);
    }

    @Test
    public void matchFilterFloatTest1() {
        filter(match("float_1", "0")).check(0);
    }

    @Test
    public void matchFilterFloatTest2() {
        filter(match("float_1", "2.0")).check(1);
    }

    @Test
    public void matchFilterFloatTest3() {
        filter(match("float_1", "2")).check(1);
    }

    @Test
    public void matchFilterFloatTest4() {
        filter(match("float_1", "2.00")).check(1);
    }

    @Test
    public void matchFilterIntegerTest1() {
        filter(match("integer_1", "-2")).check(1);
    }

    @Test
    public void matchFilterIntegerTest2() {
        filter(match("integer_1", "2")).check(0);
    }

    @Test
    public void matchFilterIntegerTest3() {
        filter(match("integer_1", "0")).check(0);
    }

    @Test
    public void matchFilterIntegerTest4() {
        filter(match("integer_1", "-1")).check(1);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchFilterUuidTest1() {
        filter(match("uuid_1", "0")).check(0);
    }

    @Test
    public void matchFilterUuidTest2() {

        filter(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).check(1);
    }

    @Test
    public void matchFilterUuidTest3() {
        filter(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).check(InvalidQueryException.class);
    }

    @Test
    public void matchFilterTimeuuidTest1() {
        filter(match("timeuuid_1", "0")).check(InvalidQueryException.class);
    }

    @Test
    public void matchFilterTimeuuidTest2() {
        filter(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).check(1);
    }

    @Test
    public void matchFilterTimeuuidTest3() {
        filter(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).check(InvalidQueryException.class);
    }

    @Test
    public void matchFilterInetFieldTest1() {
        filter(match("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void matchFilterInetFieldTest2() {
        filter(match("inet_1", "127.0.1.1")).check(1);
    }

    @Test(expected = InvalidQueryException.class)
    public void matchFilterInetFieldTest3() {
        filter(match("inet_1", "127.1.1.")).check(0);
    }

    @Test(expected = DriverException.class)
    public void matchFilterInetFieldTest4() {
        filter(match("inet_1", "")).check(0);
    }

    @Test
    public void matchFilterTextFieldTest1() {
        filter(match("text_1", "Frase")).check(1);
    }

    @Test
    public void matchFilterTextFieldTest2() {
        filter(match("text_1", "Frase*")).check(1);
    }

    @Test
    public void matchFilterTextFieldTest3() {
        filter(match("text_1", "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")).check(1);
    }

    @Test
    public void matchFilterTextFieldTest4() {
        filter(match("text_1", "")).check(0);
    }

    @Test
    public void matchFilterVarcharFieldTest1() {
        filter(match("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void matchFilterVarcharFieldTest2() {
        filter(match("varchar_1", "frase*")).check(0);
    }

    @Test
    public void matchFilterVarcharFieldTest3() {
        filter(match("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void matchFilterVarcharFieldTest4() {
        filter(match("varchar_1", "")).check(0);
    }

    @Test
    public void matchFilterListFieldTest1() {
        filter(match("list_1", "")).check(0);
    }

    @Test
    public void matchFilterListFieldTest2() {
        filter(match("list_1", "l1")).check(2);
    }

    @Test
    public void matchFilterListFieldTest3() {
        filter(match("list_1", "s1")).check(0);
    }

    @Test
    public void matchFilterSetFieldTest1() {
        filter(match("set_1", "")).check(0);
    }

    @Test
    public void matchFilterSetFieldTest2() {
        filter(match("set_1", "l1")).check(0);
    }

    @Test
    public void matchFilterSetFieldTest3() {
        filter(match("set_1", "s1")).check(2);
    }

    @Test
    public void matchFilterMapFieldTest1() {
        filter(match("map_1$k1", "")).check(0);
    }

    @Test
    public void matchFilterMapFieldTest2() {
        filter(match("map_1$k1", "l1")).check(0);
    }

    @Test
    public void matchFilterMapFieldTest3() {
        filter(match("map_1$k1", "k1")).check(0);
    }

    @Test
    public void matchFilterMapFieldTest4() {
        filter(match("map_1$k1", "v1")).check(2);
    }
}
