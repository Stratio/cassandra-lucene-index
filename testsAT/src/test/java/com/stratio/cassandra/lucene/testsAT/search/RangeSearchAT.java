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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.range;

@RunWith(JUnit4.class)
public class RangeSearchAT extends AbstractSearchAT {

    @Test
    public void rangeQueryAsciiFieldTest1() {
        query(range("ascii_1")).check(5);
    }

    @Test
    public void rangeQueryAsciiFieldTest2() {
        query(range("ascii_1").lower("a").upper("g")).check(4);
    }

    @Test
    public void rangeQueryAsciiFieldTest3() {
        query(range("ascii_1").lower("a").upper("b")).check(0);
    }

    @Test
    public void rangeQueryAsciiFieldTest4() {
        query(range("ascii_1").lower("a").upper("f")).check(0);
    }

    @Test
    public void rangeQueryAsciiFieldTest5() {
        query(range("ascii_1").lower("a").upper("f").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeQueryIntegerTest1() {
        query(range("integer_1").lower("-5").upper("5")).check(4);
    }

    @Test
    public void rangeQueryIntegerTest2() {
        query(range("integer_1").lower("-4").upper("4")).check(3);
    }

    @Test
    public void rangeQueryIntegerTest3() {
        query(range("integer_1").lower("-4").upper("-1").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryIntegerTest4() {
        query(range("integer_1")).check(5);
    }

    @Test
    public void rangeQueryBigintTest1() {
        query(range("bigint_1")).check(5);
    }

    @Test
    public void rangeQueryBigintTest2() {
        query(range("bigint_1").lower("999999999999999").upper("1000000000000001")).check(1);
    }

    @Test
    public void rangeQueryBigintTest3() {
        query(range("bigint_1").lower("1000000000000000").upper("2000000000000000")).check(0);
    }

    @Test
    public void rangeQueryBigintTest4() {
        query(range("bigint_1").lower("1000000000000000")
                               .upper("2000000000000000")
                               .includeLower(true)
                               .includeUpper(true)).check(2);
    }

    @Test
    public void rangeQueryBigintTest5() {
        query(range("bigint_1").lower("1").upper("3").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeQueryBlobTest1() {
        query(range("blob_1")).check(5);
    }

    @Test
    public void rangeQueryBlobTest2() {
        query(range("blob_1").lower("0x3E0A15").upper("0x3E0A17")).check(4);
    }

    @Test
    public void rangeQueryBlobTest3() {
        query(range("blob_1").lower("0x3E0A16").upper("0x3E0A17")).check(0);
    }

    @Test
    public void rangeQueryBlobTest4() {
        query(range("blob_1").lower("0x3E0A16").upper("0x3E0A17").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryBlobTest5() {
        query(range("blob_1").lower("0x3E0A17").upper("0x3E0A18").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeQueryBooleanTest1() {
        query(range("boolean_1")).check(5);
    }

    @Test
    public void rangeQueryDecimalTest1() {
        query(range("decimal_1")).check(5);
    }

    @Test
    public void rangeQueryDecimalTest2() {
        query(range("decimal_1").lower("1999999999.9").upper("2000000000.1")).check(1);
    }

    @Test
    public void rangeQueryDecimalTest3() {
        query(range("decimal_1").lower("2000000000.0").upper("3000000000.0")).check(0);
    }

    @Test
    public void rangeQueryDecimalTest4() {
        query(range("decimal_1").lower("2000000000.0")
                                .upper("3000000000.0")
                                .includeLower(true)
                                .includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryDecimalTest5() {
        query(range("decimal_1").lower("2000000000.000001").upper("2000000000.181235")).check(0);
    }

    @Test
    public void rangeQueryDoubleTest1() {
        query(range("double_1")).check(5);
    }

    @Test
    public void rangeQueryDoubleTest2() {
        query(range("double_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void rangeQueryDoubleTest3() {
        query(range("double_1").lower("2.0").upper("3.0")).check(0);
    }

    @Test
    public void rangeQueryDoubleTest4() {
        query(range("double_1").lower("2.0").upper("3.0").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryDoubleTest5() {
        query(range("double_1").lower("7.0").upper("10.0").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeQueryFloatTest1() {
        query(range("float_1")).check(5);
    }

    @Test
    public void rangeQueryFloatTest2() {
        query(range("float_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void rangeQueryFloatTest3() {
        query(range("float_1").lower("1.0").upper("2.0")).check(0);
    }

    @Test
    public void rangeQueryFloatTest4() {
        query(range("float_1").lower("1.0").upper("2.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void rangeQueryFloatTest5() {
        query(range("float_1").lower("7.0").upper("9.0")).check(0);
    }

    @Test
    public void rangeQueryUuidTest1() {
        query(range("uuid_1")).check(5);
    }

    @Test
    public void rangeQueryUuidTest2() {
        query(range("uuid_1").lower("1").upper("9")).check(InvalidQueryException.class);
    }

    @Test
    public void rangeQueryUuidTest3() {
        query(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                             .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")).check(0);
    }

    @Test
    public void rangeQueryUuidTest4() {
        query(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                             .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")
                             .includeLower(true)
                             .includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryTimeuuidTest1() {
        query(range("timeuuid_1")).check(5);
    }

    @Test
    public void rangeQueryTimeuuidTest2() {
        query(range("timeuuid_1").lower("a").upper("z")).check(InvalidQueryException.class);
    }

    @Test
    public void rangeQueryTimeuuidTest3() {
        query(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                 .upper("a4a70900-24e1-11df-8924-001ff3591713")).check(0);
    }

    @Test
    public void rangeQueryTimeuuidTest4() {
        query(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                 .upper("a4a70900-24e1-11df-8924-001ff3591713")
                                 .includeLower(true)
                                 .includeUpper(true)).check(4);
    }

    @Test
    public void rangeQueryInetFieldTest1() {
        query(range("inet_1")).check(5);
    }

    @Test
    public void rangeQueryInetFieldTest2() {
        query(range("inet_1").lower("127.0.0.0").upper("127.1.0.0")).check(2);
    }

    @Test
    public void rangeQueryInetFieldTest3() {
        query(range("inet_1").lower("127.0.0.0").upper("127.1.0.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void rangeQueryInetFieldTest4() {
        query(range("inet_1").lower("192.168.0.0").upper("192.168.0.1")).check(0);
    }

    @Test
    public void rangeQueryTextFieldTest1() {
        query(range("text_1")).check(5);
    }

    @Test
    public void rangeQueryTextFieldTest2() {
        query(range("text_1").lower("frase").upper("g")).check(3);
    }

    @Test
    public void rangeQueryTextFieldTest3() {
        query(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                             .upper("g")).check(1);
    }

    @Test
    public void rangeQueryTextFieldTest4() {
        query(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                             .upper("g")
                             .includeLower(true)
                             .includeUpper(true)).check(2);
    }

    @Test
    public void rangeQueryTextFieldTest5() {
        query(range("text_1").lower("G").upper("H").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeQueryVarcharFieldTest1() {
        query(range("varchar_1")).check(5);
    }

    @Test
    public void rangeQueryVarcharFieldTest2() {
        query(range("varchar_1").lower("frase").upper("g")).check(4);
    }

    @Test
    public void rangeQueryVarcharFieldTest3() {
        query(range("varchar_1").lower("frasesencillasinespaciosperomaslarga").upper("g")).check(0);
    }

    @Test
    public void rangeQueryVarcharFieldTest4() {
        query(range("varchar_1").lower("frasesencillasinespaciosperomaslarga")
                                .upper("gH")
                                .includeLower(true)
                                .includeUpper(true)).check(2);
    }

    @Test
    public void rangeQueryVarcharFieldTest5() {
        query(range("varchar_1").lower("g").upper("h")).check(0);
    }

    @Test
    public void rangeQueryListFieldTest1() {
        query(range("list_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void rangeQueryListFieldTest2() {
        query(range("list_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void rangeQueryListFieldTest3() {
        query(range("list_1").lower("a2").upper("l1")).check(0);
    }

    @Test
    public void rangeQuerySetFieldTest1() {
        query(range("set_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void rangeQuerySetFieldTest2() {
        query(range("set_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void rangeQuerySetFieldTest3() {
        query(range("set_1").lower("a1").upper("z1")).check(5);
    }

    @Test
    public void rangeQueryMapFieldTest1() {
        query(range("map_1$k1").lower("a").upper("z")).check(2);
    }

    @Test
    public void rangeQueryMapFieldTest2() {
        query(range("map_1$k1").lower("a1").upper("z9")).check(2);
    }

    @Test
    public void rangeQueryMapFieldTest3() {
        query(range("map_1$k1").lower("a1").upper("k9")).check(0);
    }

    @Test
    public void rangeQueryMapFieldTest4() {
        query(range("map_1$k1").lower("a1").upper("k1")).check(0);
    }

    @Test
    public void rangeFilterAsciiFieldTest1() {
        filter(range("ascii_1")).check(5);
    }

    @Test
    public void rangeFilterAsciiFieldTest2() {
        filter(range("ascii_1").lower("a").upper("g")).check(4);
    }

    @Test
    public void rangeFilterAsciiFieldTest3() {
        filter(range("ascii_1").lower("a").upper("b")).check(0);
    }

    @Test
    public void rangeFilterAsciiFieldTest4() {
        filter(range("ascii_1").lower("a").upper("f")).check(0);
    }

    @Test
    public void rangeFilterAsciiFieldTest5() {
        filter(range("ascii_1").lower("a").upper("f").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeFilterIntegerTest1() {
        filter(range("integer_1").lower("-5").upper("5")).check(4);
    }

    @Test
    public void rangeFilterIntegerTest2() {
        filter(range("integer_1").lower("-4").upper("4")).check(3);
    }

    @Test
    public void rangeFilterIntegerTest3() {
        filter(range("integer_1").lower("-4").upper("-1").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterIntegerTest4() {
        filter(range("integer_1")).check(5);
    }

    @Test
    public void rangeFilterBigintTest1() {
        filter(range("bigint_1")).check(5);
    }

    @Test
    public void rangeFilterBigintTest2() {
        filter(range("bigint_1").lower("999999999999999").upper("1000000000000001")).check(1);
    }

    @Test
    public void rangeFilterBigintTest3() {
        filter(range("bigint_1").lower("1000000000000000").upper("2000000000000000")).check(0);
    }

    @Test
    public void rangeFilterBigintTest4() {
        filter(range("bigint_1").lower("1000000000000000")
                                .upper("2000000000000000")
                                .includeLower(true)
                                .includeUpper(true)).check(2);
    }

    @Test
    public void rangeFilterBigintTest5() {
        filter(range("bigint_1").lower("1").upper("3").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeFilterBlobTest1() {
        filter(range("blob_1")).check(5);
    }

    @Test
    public void rangeFilterBlobTest2() {
        filter(range("blob_1").lower("0x3E0A15").upper("0x3E0A17")).check(4);
    }

    @Test
    public void rangeFilterBlobTest3() {
        filter(range("blob_1").lower("0x3E0A16").upper("0x3E0A17")).check(0);
    }

    @Test
    public void rangeFilterBlobTest4() {
        filter(range("blob_1").lower("0x3E0A16").upper("0x3E0A17").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterBlobTest5() {
        filter(range("blob_1").lower("0x3E0A17").upper("0x3E0A18").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeFilterBooleanTest1() {
        filter(range("boolean_1")).check(5);
    }

    @Test
    public void rangeFilterDecimalTest1() {
        filter(range("decimal_1")).check(5);
    }

    @Test
    public void rangeFilterDecimalTest2() {
        filter(range("decimal_1").lower("1999999999.9").upper("2000000000.1")).check(1);
    }

    @Test
    public void rangeFilterDecimalTest3() {
        filter(range("decimal_1").lower("2000000000.0").upper("3000000000.0")).check(0);
    }

    @Test
    public void rangeFilterDecimalTest4() {
        filter(range("decimal_1").lower("2000000000.0")
                                 .upper("3000000000.0")
                                 .includeLower(true)
                                 .includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterDecimalTest5() {
        filter(range("decimal_1").lower("2000000000.000001").upper("2000000000.181235")).check(0);
    }

    @Test
    public void rangeFilterDoubleTest1() {
        filter(range("double_1")).check(5);
    }

    @Test
    public void rangeFilterDoubleTest2() {
        filter(range("double_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void rangeFilterDoubleTest3() {
        filter(range("double_1").lower("2.0").upper("3.0")).check(0);
    }

    @Test
    public void rangeFilterDoubleTest4() {
        filter(range("double_1").lower("2.0").upper("3.0").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterDoubleTest5() {
        filter(range("double_1").lower("7.0").upper("10.0").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeFilterFloatTest1() {
        filter(range("float_1")).check(5);
    }

    @Test
    public void rangeFilterFloatTest2() {
        filter(range("float_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void rangeFilterFloatTest3() {
        filter(range("float_1").lower("1.0").upper("2.0")).check(0);
    }

    @Test
    public void rangeFilterFloatTest4() {
        filter(range("float_1").lower("1.0").upper("2.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void rangeFilterFloatTest5() {
        filter(range("float_1").lower("7.0").upper("9.0")).check(0);
    }

    @Test
    public void rangeFilterUuidTest1() {
        filter(range("uuid_1")).check(5);
    }

    @Test
    public void rangeFilterUuidTest2() {
        filter(range("uuid_1").lower("1").upper("9")).check(InvalidQueryException.class);
    }

    @Test
    public void rangeFilterUuidTest3() {
        filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                              .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")).check(0);
    }

    @Test
    public void rangeFilterUuidTest4() {
        filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                              .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")
                              .includeLower(true)
                              .includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterTimeuuidTest1() {
        filter(range("timeuuid_1")).check(5);
    }

    @Test(expected = InvalidQueryException.class)
    public void rangeFilterTimeuuidTest2() {
        filter(range("timeuuid_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void rangeFilterTimeuuidTest3() {
        filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                  .upper("a4a70900-24e1-11df-8924-001ff3591713")).check(0);
    }

    @Test
    public void rangeFilterTimeuuidTest4() {
        filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                  .upper("a4a70900-24e1-11df-8924-001ff3591713")
                                  .includeLower(true)
                                  .includeUpper(true)).check(4);
    }

    @Test
    public void rangeFilterInetFieldTest1() {
        filter(range("inet_1")).check(5);
    }

    @Test
    public void rangeFilterInetFieldTest2() {
        filter(range("inet_1").lower("127.0.0.0").upper("127.1.0.0")).check(2);
    }

    @Test
    public void rangeFilterInetFieldTest3() {
        filter(range("inet_1").lower("127.0.0.0").upper("127.1.0.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void rangeFilterInetFieldTest4() {
        filter(range("inet_1").lower("192.168.0.0").upper("192.168.0.1")).check(0);
    }

    @Test
    public void rangeFilterTextFieldTest1() {
        filter(range("text_1")).check(5);
    }

    @Test
    public void rangeFilterTextFieldTest2() {
        filter(range("text_1").lower("frase").upper("g")).check(3);
    }

    @Test
    public void rangeFilterTextFieldTest3() {
        filter(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                              .upper("g")).check(1);
    }

    @Test
    public void rangeFilterTextFieldTest4() {
        filter(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                              .upper("g")
                              .includeLower(true)
                              .includeUpper(true)).check(2);
    }

    @Test
    public void rangeFilterTextFieldTest5() {
        filter(range("text_1").lower("G").upper("H").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeFilterVarcharFieldTest1() {
        filter(range("varchar_1")).check(5);
    }

    @Test
    public void rangeFilterVarcharFieldTest2() {
        filter(range("varchar_1").lower("frase").upper("g")).check(4);
    }

    @Test
    public void rangeFilterVarcharFieldTest3() {
        filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga").upper("g")).check(0);
    }

    @Test
    public void rangeFilterVarcharFieldTest4() {
        filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga")
                                 .upper("gH")
                                 .includeLower(true)
                                 .includeUpper(true)).check(2);
    }

    @Test
    public void rangeFilterVarcharFieldTest5() {
        filter(range("varchar_1").lower("g").upper("h")).check(0);
    }

    @Test
    public void rangeFilterListFieldTest1() {
        filter(range("list_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void rangeFilterListFieldTest2() {
        filter(range("list_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void rangeFilterListFieldTest3() {
        filter(range("list_1").lower("a2").upper("l1")).check(0);
    }

    @Test
    public void rangeFilterSetFieldTest1() {
        filter(range("set_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void rangeFilterSetFieldTest2() {
        filter(range("set_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void rangeFilterSetFieldTest3() {
        filter(range("set_1").lower("a1").upper("z1")).check(5);
    }

    @Test
    public void rangeFilterMapFieldTest1() {
        filter(range("map_1$k1").lower("a").upper("z")).check(2);
    }

    @Test
    public void rangeFilterMapFieldTest2() {
        filter(range("map_1$k1").lower("a1").upper("z9")).check(2);
    }

    @Test
    public void rangeFilterMapFieldTest3() {
        filter(range("map_1$k1").lower("a1").upper("k9")).check(0);
    }

    @Test
    public void rangeFilterMapFieldTest4() {
        filter(range("map_1$k1").lower("a1").upper("k1")).check(0);
    }
}
