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
import com.stratio.cassandra.lucene.builder.search.condition.MatchCondition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@RunWith(JUnit4.class)
public class MatchSearchWithDocValuesAT extends AbstractSearchAT {

    private static MatchCondition match(String field, Object value) {
        return new MatchCondition(field, value).docValues(true);
    }

    @Test
    public void matchAsciiTest1() {
        filter(match("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void matchAsciiTest2() {
        filter(match("ascii_1", "frase")).check(0);
    }

    @Test
    public void matchAsciiTest3() {
        filter(match("ascii_1", "frase tipo asciii")).check(0);
    }

    @Test
    public void matchAsciiTest4() {
        filter(match("ascii_1", "")).check(0);
    }

    @Test
    public void matchAsciiTest5() {
        filter(match("ascii_1", "frase tipo asci")).check(0);
    }

    @Test
    public void matchBigintTest2() {
        filter(match("bigint_1", "1000000000000000")).check(1);
    }

    @Test
    public void matchBigintTest3() {
        filter(match("bigint_1", "3000000000000000")).check(3);
    }

    @Test
    public void matchBigintTest4() {
        filter(match("bigint_1", "10000000000000000")).check(0);
    }

    @Test
    public void matchBigintTest5() {
        filter(match("bigint_1", "100000000000000")).check(0);
    }

    @Test
    public void matchBlobTest1() {
        filter(match("blob_1", "")).check(0);
    }

    @Test
    public void matchBlobTest2() {
        filter(match("blob_1", "3E0A16")).check(4);
    }

    @Test
    public void matchBlobTest3() {
        String msg = "Field 'blob_1' requires an hex string, but found '3E0A161'";
        filter(match("blob_1", "3E0A161")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchBlobTest4() {
        String msg = "Field 'blob_1' requires an hex string, but found '3E0A1'";
        filter(match("blob_1", "3E0A1")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchBlobTest5() {
        filter(match("blob_1", "3E0A15")).check(1);
    }

    @Test
    public void matchBooleanTest1() {
        String msg = "Boolean field 'boolean_1' requires either 'true' or 'false', but found ''";
        filter(match("boolean_1", "")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchBooleanTest3() {
        filter(match("boolean_1", "true")).check(4);
    }

    @Test
    public void matchBooleanTest4() {
        filter(match("boolean_1", "false")).check(1);
    }

    @Test
    public void matchBooleanTest5() {
        String msg = "Boolean field 'boolean_1' requires either 'true' or 'false', but found 'else'";
        filter(match("boolean_1", "else")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchDecimalTest2() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchDecimalTest3() {
        filter(match("decimal_1", "300000000.0")).check(0);
    }

    @Test
    public void matchDecimalTest4() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void matchDecimalTest5() {
        filter(match("decimal_1", "1000000000.0")).check(1);
    }

    @Test
    public void matchDateTest1() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchDateTest2() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchDateTest3() {
        filter(match("date_1", "1970/01/01 00:00:00.000")).check(0);
    }

    @Test
    public void matchDateTest4() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void matchDoubleTest1() {
        filter(match("double_1", "0")).check(0);
    }

    @Test
    public void matchDoubleTest2() {
        filter(match("double_1", "2.0")).check(1);
    }

    @Test
    public void matchDoubleTest3() {
        filter(match("double_1", "2")).check(1);
    }

    @Test
    public void matchDoubleTest4() {
        filter(match("double_1", "2.00")).check(1);
    }

    @Test
    public void matchFloatTest1() {
        filter(match("float_1", "0")).check(0);
    }

    @Test
    public void matchFloatTest2() {
        filter(match("float_1", "2.0")).check(1);
    }

    @Test
    public void matchFloatTest3() {
        filter(match("float_1", "2")).check(1);
    }

    @Test
    public void matchFloatTest4() {
        filter(match("float_1", "2.00")).check(1);
    }

    @Test
    public void matchIntegerTest1() {
        filter(match("integer_1", "-2")).check(1);
    }

    @Test
    public void matchIntegerTest2() {
        filter(match("integer_1", "2")).check(0);
    }

    @Test
    public void matchIntegerTest3() {
        filter(match("integer_1", "0")).check(0);
    }

    @Test
    public void matchIntegerTest4() {
        filter(match("integer_1", "-1")).check(1);
    }

    @Test
    public void matchUuidTest1() {
        String msg = "Field 'uuid_1' with value '0' can not be parsed as UUID";
        filter(match("uuid_1", "0")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchUuidTest2() {
        filter(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).check(1);
    }

    @Test
    public void matchUuidTest3() {
        String msg = "Field 'uuid_1' with value '60297440-b4fa-11e3-0002a5d5c51b' can not be parsed as UUID";
        filter(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchTimeuuidTest1() {
        String msg = "Field 'timeuuid_1' with value '0' can not be parsed as UUID";
        filter(match("timeuuid_1", "0")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchTimeuuidTest2() {
        filter(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).check(1);
    }

    @Test
    public void matchTimeuuidTest3() {
        String msg = "Field 'timeuuid_1' with value 'a4a70900-24e1-11df-001ff3591711' can not be parsed as UUID";
        filter(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchInetTest1() {
        filter(match("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void matchInetTest2() {
        filter(match("inet_1", "127.0.1.1")).check(1);
    }

    @Test
    public void matchInetTest3() {
        String msg = "Field 'inet_1' with value '127.1.1.' can not be parsed as an inet address";
        filter(match("inet_1", "127.1.1.")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchInetTest4() {
        String msg = "Field 'inet_1' with value '' can not be parsed as an inet address";
        filter(match("inet_1", "")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void matchTextTest1() {
        filter(match("text_1", "Frase")).check(InvalidQueryException.class, UNSUPPORTED_DOC_VALUES);
    }

    @Test
    public void matchTextTest2() {
        filter(match("text_1", "Frase*")).check(InvalidQueryException.class, UNSUPPORTED_DOC_VALUES);
    }

    @Test
    public void matchTextTest3() {
        filter(match("text_1", "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")).check(
                InvalidQueryException.class,
                UNSUPPORTED_DOC_VALUES);
    }

    @Test
    public void matchTextTest4() {
        filter(match("text_1", "")).check(InvalidQueryException.class, UNSUPPORTED_DOC_VALUES);
    }

    @Test
    public void matchVarcharTest1() {
        filter(match("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void matchVarcharTest2() {
        filter(match("varchar_1", "frase*")).check(0);
    }

    @Test
    public void matchVarcharTest3() {
        filter(match("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void matchVarcharTest4() {
        filter(match("varchar_1", "")).check(0);
    }

    @Test
    public void matchListTest1() {
        filter(match("string_list", "")).check(0);
    }

    @Test
    public void matchListTest2() {
        filter(match("string_list", "l1")).check(2);
    }

    @Test
    public void matchListTest3() {
        filter(match("string_list", "s1")).check(0);
    }

    @Test
    public void matchSetTest1() {
        filter(match("string_set", "")).check(0);
    }

    @Test
    public void matchSetTest2() {
        filter(match("string_set", "l1")).check(0);
    }

    @Test
    public void matchSetTest3() {
        filter(match("string_set", "s1")).check(2);
    }

    @Test
    public void matchMapTest1() {
        filter(match("string_map$k1", "")).check(0);
    }

    @Test
    public void matchMapTest2() {
        filter(match("string_map$k1", "l1")).check(0);
    }

    @Test
    public void matchMapTest3() {
        filter(match("string_map$k1", "k1")).check(0);
    }

    @Test
    public void matchMapTest4() {
        filter(match("string_map$k1", "v1")).check(2);
    }
}
