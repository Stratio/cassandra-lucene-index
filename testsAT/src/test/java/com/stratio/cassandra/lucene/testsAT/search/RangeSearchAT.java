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
package com.stratio.cassandra.lucene.testsAT.search;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.range;

@RunWith(JUnit4.class)
public class RangeSearchAT extends AbstractSearchAT {

    @Test
    public void testRangeAsciiField1() {
        filter(range("ascii_1")).check(5);
    }

    @Test
    public void testRangeAsciiField2() {
        filter(range("ascii_1").lower("a").upper("g")).check(4);
    }

    @Test
    public void testRangeAsciiField3() {
        filter(range("ascii_1").lower("a").upper("b")).check(0);
    }

    @Test
    public void testRangeAsciiField4() {
        filter(range("ascii_1").lower("a").upper("f")).check(0);
    }

    @Test
    public void testRangeAsciiField5() {
        filter(range("ascii_1").lower("a").upper("f").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void rangeIntegerTest1() {
        filter(range("integer_1").lower("-5").upper("5")).check(4);
    }

    @Test
    public void rangeIntegerTest2() {
        filter(range("integer_1").lower("-4").upper("4")).check(3);
    }

    @Test
    public void rangeIntegerTest3() {
        filter(range("integer_1").lower("-4").upper("-1").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void rangeIntegerTest4() {
        filter(range("integer_1")).check(5);
    }

    @Test
    public void testRangeBigintField1() {
        filter(range("bigint_1")).check(5);
    }

    @Test
    public void testRangeBigintField2() {
        filter(range("bigint_1").lower("999999999999999").upper("1000000000000001")).check(1);
    }

    @Test
    public void testRangeBigintField3() {
        filter(range("bigint_1").lower("1000000000000000").upper("2000000000000000")).check(0);
    }

    @Test
    public void testRangeBigintField4() {
        filter(range("bigint_1").lower("1000000000000000")
                                .upper("2000000000000000")
                                .includeLower(true)
                                .includeUpper(true)).check(2);
    }

    @Test
    public void testRangeBigintField5() {
        filter(range("bigint_1").lower("1").upper("3").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void testRangeBlobField1() {
        filter(range("blob_1")).check(5);
    }

    @Test
    public void testRangeBlobField2() {
        filter(range("blob_1").lower("0x3E0A15").upper("0x3E0A17")).check(4);
    }

    @Test
    public void testRangeBlobField3() {
        filter(range("blob_1").lower("0x3E0A16").upper("0x3E0A17")).check(0);
    }

    @Test
    public void testRangeBlobField4() {
        filter(range("blob_1").lower("0x3E0A16").upper("0x3E0A17").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void testRangeBlobField5() {
        filter(range("blob_1").lower("0x3E0A17").upper("0x3E0A18").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void testRangeBooleanField1() {
        filter(range("boolean_1")).check(5);
    }

    @Test
    public void testRangeDecimalField1() {
        filter(range("decimal_1")).check(5);
    }

    @Test
    public void testRangeDecimalField2() {
        filter(range("decimal_1").lower("1999999999.9").upper("2000000000.1")).check(1);
    }

    @Test
    public void testRangeDecimalField3() {
        filter(range("decimal_1").lower("2000000000.0").upper("3000000000.0")).check(0);
    }

    @Test
    public void testRangeDecimalField4() {
        filter(range("decimal_1").lower("2000000000.0")
                                 .upper("3000000000.0")
                                 .includeLower(true)
                                 .includeUpper(true)).check(4);
    }

    @Test
    public void testRangeDecimalField5() {
        filter(range("decimal_1").lower("2000000000.000001").upper("2000000000.181235")).check(0);
    }

    @Test
    public void testRangeDoubleField1() {
        filter(range("double_1")).check(5);
    }

    @Test
    public void testRangeDoubleField2() {
        filter(range("double_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void testRangeDoubleField3() {
        filter(range("double_1").lower("2.0").upper("3.0")).check(0);
    }

    @Test
    public void testRangeDoubleField4() {
        filter(range("double_1").lower("2.0").upper("3.0").includeLower(true).includeUpper(true)).check(4);
    }

    @Test
    public void testRangeDoubleField5() {
        filter(range("double_1").lower("7.0").upper("10.0").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void testRangeFloatField1() {
        filter(range("float_1")).check(5);
    }

    @Test
    public void testRangeFloatField2() {
        filter(range("float_1").lower("1.9").upper("2.1")).check(1);
    }

    @Test
    public void testRangeFloatField3() {
        filter(range("float_1").lower("1.0").upper("2.0")).check(0);
    }

    @Test
    public void testRangeFloatField4() {
        filter(range("float_1").lower("1.0").upper("2.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void testRangeFloatField5() {
        filter(range("float_1").lower("7.0").upper("9.0")).check(0);
    }

    @Test
    public void testRangeUUIDField1() {
        filter(range("uuid_1")).check(5);
    }

    @Test
    public void testRangeUUIDField2() {
        String msg = "Field 'uuid_1' with value '1' can not be parsed as UUID";
        filter(range("uuid_1").lower("1").upper("9")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void testRangeUUIDField3() {
        filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                              .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")).check(0);
    }

    @Test
    public void testRangeUUIDField4() {
        filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                              .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")
                              .includeLower(true)
                              .includeUpper(true)).check(4);
    }

    @Test
    public void testRangeTimeUUIDField1() {
        filter(range("timeuuid_1")).check(5);
    }

    @Test
    public void testRangeTimeUUIDField2() {
        filter(range("timeuuid_1").lower("a").upper("z")).check(InvalidQueryException.class,
                                                                "Field 'timeuuid_1' with value 'a' can not be parsed as UUID");
    }

    @Test
    public void testRangeTimeUUIDField3() {
        filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                  .upper("a4a70900-24e1-11df-8924-001ff3591713")).check(0);
    }

    @Test
    public void testRangeTimeUUIDField4() {
        filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                  .upper("a4a70900-24e1-11df-8924-001ff3591713")
                                  .includeLower(true)
                                  .includeUpper(true)).check(4);
    }

    @Test
    public void testRangeInetField1() {
        filter(range("inet_1")).check(5);
    }

    @Test
    public void testRangeInetField2() {
        filter(range("inet_1").lower("127.0.0.0").upper("127.1.0.0")).check(2);
    }

    @Test
    public void testRangeInetField3() {
        filter(range("inet_1").lower("127.0.0.0").upper("127.1.0.0").includeLower(true).includeUpper(true)).check(2);
    }

    @Test
    public void testRangeInetField4() {
        filter(range("inet_1").lower("192.168.0.0").upper("192.168.0.1")).check(0);
    }

    @Test
    public void testRangeTextField1() {
        filter(range("text_1")).check(5);
    }

    @Test
    public void testRangeTextField2() {
        filter(range("text_1").lower("frase").upper("g")).check(3);
    }

    @Test
    public void testRangeTextField3() {
        filter(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                              .upper("g")).check(1);
    }

    @Test
    public void testRangeTextField4() {
        filter(range("text_1").lower("frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                              .upper("g")
                              .includeLower(true)
                              .includeUpper(true)).check(2);
    }

    @Test
    public void testRangeTextField5() {
        filter(range("text_1").lower("G").upper("H").includeLower(true).includeUpper(true)).check(0);
    }

    @Test
    public void testRangeVarcharField1() {
        filter(range("varchar_1")).check(5);
    }

    @Test
    public void testRangeVarcharField2() {
        filter(range("varchar_1").lower("frase").upper("g")).check(4);
    }

    @Test
    public void testRangeVarcharField3() {
        filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga").upper("g")).check(0);
    }

    @Test
    public void testRangeVarcharField4() {
        filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga")
                                 .upper("gH")
                                 .includeLower(true)
                                 .includeUpper(true)).check(2);
    }

    @Test
    public void testRangeVarcharField5() {
        filter(range("varchar_1").lower("g").upper("h")).check(0);
    }

    @Test
    public void testRangeListField1() {
        filter(range("list_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void testRangeListField2() {
        filter(range("list_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void testRangeListField3() {
        filter(range("list_1").lower("a2").upper("l1")).check(0);
    }

    @Test
    public void testRangeSetField1() {
        filter(range("set_1").lower("a").upper("z")).check(5);
    }

    @Test
    public void testRangeSetField2() {
        filter(range("set_1").lower("a1").upper("z9")).check(5);
    }

    @Test
    public void testRangeSetField3() {
        filter(range("set_1").lower("a1").upper("z1")).check(5);
    }

    @Test
    public void testRangeMapField1() {
        filter(range("map_1$k1").lower("a").upper("z")).check(2);
    }

    @Test
    public void testRangeMapField2() {
        filter(range("map_1$k1").lower("a1").upper("z9")).check(2);
    }

    @Test
    public void testRangeMapField3() {
        filter(range("map_1$k1").lower("a1").upper("k9")).check(0);
    }

    @Test
    public void testRangeMapField4() {
        filter(range("map_1$k1").lower("a1").upper("k1")).check(0);
    }

    @Test
    public void testRangeMapFieldWithAlias1() {
        filter(range("string_map$k1").lower("a").upper("z")).check(2);
    }

    @Test
    public void testRangeMapFieldWithAlias2() {
        filter(range("string_map$k1").lower("a1").upper("z9")).check(2);
    }

    @Test
    public void testRangeMapFieldWithAlias3() {
        filter(range("string_map$k1").lower("a1").upper("k9")).check(0);
    }

    @Test
    public void testRangeMapFieldWithAlias4() {
        filter(range("string_map$k1").lower("a1").upper("k1")).check(0);
    }
}
