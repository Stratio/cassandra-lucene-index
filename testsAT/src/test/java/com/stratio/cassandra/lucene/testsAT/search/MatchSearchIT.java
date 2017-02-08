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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.stratio.cassandra.lucene.builder.Builder.match;

@RunWith(JUnit4.class)
public class MatchSearchIT extends AbstractSearchIT {

    @Test
    public void testMatchAsciiField1() {
        filter(match("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void testMatchAsciiField2() {
        filter(match("ascii_1", "frase")).check(0);
    }

    @Test
    public void testMatchAsciiField3() {
        filter(match("ascii_1", "frase tipo asciii")).check(0);
    }

    @Test
    public void testMatchAsciiField4() {
        filter(match("ascii_1", "")).check(0);
    }

    @Test
    public void testMatchAsciiField5() {
        filter(match("ascii_1", "frase tipo asci")).check(0);
    }

    @Test
    public void testMatchBigintField2() {
        filter(match("bigint_1", "1000000000000000")).check(1);
    }

    @Test
    public void testMatchBigintField3() {
        filter(match("bigint_1", "3000000000000000")).check(3);
    }

    @Test
    public void testMatchBigintField4() {
        filter(match("bigint_1", "10000000000000000")).check(0);
    }

    @Test
    public void testMatchBigintField5() {
        filter(match("bigint_1", "100000000000000")).check(0);
    }

    @Test
    public void testMatchBlobField1() {
        filter(match("blob_1", "")).check(0);
    }

    @Test
    public void testMatchBlobField2() {
        filter(match("blob_1", "3E0A16")).check(4);
    }

    @Test
    public void testMatchBlobField3() {
        filter(match("blob_1", "3E0A161")).check(InvalidQueryException.class,
                "Field 'blob_1' requires an hex string, but found '3E0A161'");
    }

    @Test
    public void testMatchBlobField4() {
        filter(match("blob_1", "3E0A1")).check(InvalidQueryException.class,
                "Field 'blob_1' requires an hex string, but found '3E0A1'");
    }

    @Test
    public void testMatchBlobField5() {
        filter(match("blob_1", "3E0A15")).check(1);
    }

    @Test
    public void testMatchBooleanField1() {
        filter(match("boolean_1", "")).check(InvalidQueryException.class,
                "Boolean field 'boolean_1' requires either 'true' or 'false', but found ''");
    }

    @Test
    public void testMatchBooleanField3() {
        filter(match("boolean_1", "true")).check(4);
    }

    @Test
    public void testMatchBooleanField4() {
        filter(match("boolean_1", "false")).check(1);
    }

    @Test
    public void testMatchBooleanField5() {
        filter(match("boolean_1", "else")).check(InvalidQueryException.class,
                "Boolean field 'boolean_1' requires either 'true' or 'false', but found 'else'");
    }

    @Test
    public void testMatchDecimalField2() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void testMatchDecimalField3() {
        filter(match("decimal_1", "300000000.0")).check(0);
    }

    @Test
    public void testMatchDecimalField4() {
        filter(match("decimal_1", "3000000000.0")).check(3);
    }

    @Test
    public void testMatchDecimalField5() {
        filter(match("decimal_1", "1000000000.0")).check(1);
    }

    @Test
    public void testMatchDateField1() {
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void testMatchDateField2() {
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();
        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void testMatchDateField3() {
        filter(match("date_1", "1970/01/01 00:00:00.000")).check(0);
    }

    @Test
    public void testMatchDateField4() {
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();
        filter(match("date_1", df.format(date))).check(0);
    }

    @Test
    public void testMatchDouble1() {
        filter(match("double_1", "0")).check(0);
    }

    @Test
    public void testMatchDouble2() {
        filter(match("double_1", "2.0")).check(1);
    }

    @Test
    public void testMatchDouble3() {
        filter(match("double_1", "2")).check(1);
    }

    @Test
    public void testMatchDouble4() {
        filter(match("double_1", "2.00")).check(1);
    }

    @Test
    public void testMatchFloatField1() {
        filter(match("float_1", "0")).check(0);
    }

    @Test
    public void testMatchFloatField2() {
        filter(match("float_1", "2.0")).check(1);
    }

    @Test
    public void testMatchFloatField3() {
        filter(match("float_1", "2")).check(1);
    }

    @Test
    public void testMatchFloatField4() {
        filter(match("float_1", "2.00")).check(1);
    }

    @Test
    public void testMatchIntegerField1() {
        filter(match("integer_1", "-2")).check(1);
    }

    @Test
    public void testMatchIntegerField2() {
        filter(match("integer_1", "2")).check(0);
    }

    @Test
    public void testMatchIntegerField3() {
        filter(match("integer_1", "0")).check(0);
    }

    @Test
    public void testMatchIntegerField4() {
        filter(match("integer_1", "-1")).check(1);
    }

    @Test
    public void testMatchUUIDField1() {
        filter(match("uuid_1", "0")).check(InvalidQueryException.class,
                "Field 'uuid_1' with value '0' can not be parsed as UUID");
    }

    @Test
    public void testMatchUUIDField2() {
        filter(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).check(1);
    }

    @Test
    public void testMatchUUIDField3() {
        String msg
                = "Field 'uuid_1' with value '60297440-b4fa-11e3-0002a5d5c51b' can not be parsed as UUID";
        filter(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void testMatchTimeUUIDField1() {
        String msg = "Field 'timeuuid_1' with value '0' can not be parsed as UUID";
        filter(match("timeuuid_1", "0")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void testMatchTimeUUIDField2() {
        filter(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).check(1);
    }

    @Test
    public void testMatchTimeUUIDField3() {
        String msg = "Field 'timeuuid_1' with value 'a4a70900-24e1-11df-001ff3591711' can not be parsed as UUID";
        filter(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).check(InvalidQueryException.class, msg);
    }

    @Test
    public void testMatchInetField1() {
        filter(match("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void testMatchInetField2() {
        filter(match("inet_1", "127.0.1.1")).check(1);
    }

    @Test
    public void testMatchInetField3() {
        filter(match("inet_1", "127.1.1.")).check(InvalidQueryException.class,
                "Field 'inet_1' with value '127.1.1.' can not be parsed as an inet address");
    }

    @Test
    public void testMatchInetField4() {
        filter(match("inet_1", "")).check(InvalidQueryException.class,
                "Field 'inet_1' with value '' can not be parsed as an inet address");
    }

    @Test
    public void testMatchTextField1() {
        filter(match("text_1", "Frase")).check(1);
    }

    @Test
    public void testMatchTextField2() {
        filter(match("text_1", "Frase*")).check(1);
    }

    @Test
    public void testMatchTextField3() {
        filter(match("text_1", "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")).check(1);
    }

    @Test
    public void testMatchTextField4() {
        filter(match("text_1", "")).check(0);
    }

    @Test
    public void testMatchVarcharField1() {
        filter(match("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void testMatchVarcharField2() {
        filter(match("varchar_1", "frase*")).check(0);
    }

    @Test
    public void testMatchVarcharField3() {
        filter(match("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void testMatchVarcharField4() {
        filter(match("varchar_1", "")).check(0);
    }

    @Test
    public void testMatchListField1() {
        filter(match("list_1", "")).check(0);
    }

    @Test
    public void testMatchListField2() {
        filter(match("list_1", "l1")).check(2);
    }

    @Test
    public void testMatchListField3() {
        filter(match("list_1", "s1")).check(0);
    }

    @Test
    public void testMatchSetField1() {
        filter(match("set_1", "")).check(0);
    }

    @Test
    public void testMatchSetField2() {
        filter(match("set_1", "l1")).check(0);
    }

    @Test
    public void testMatchSetField3() {
        filter(match("set_1", "s1")).check(2);
    }

    @Test
    public void testMatchMapField1() {
        filter(match("map_1$k1", "")).check(0);
    }

    @Test
    public void testMatchMapField2() {
        filter(match("map_1$k1", "l1")).check(0);
    }

    @Test
    public void testMatchMapField3() {
        filter(match("map_1$k1", "k1")).check(0);
    }

    @Test
    public void testMatchMapField4() {
        filter(match("map_1$k1", "v1")).check(2);
    }

    @Test
    public void testMatchMapFieldWithAlias1() {
        filter(match("string_map$k1", "")).check(0);
    }

    @Test
    public void testMatchMapFieldWithAlias2() {
        filter(match("string_map$k1", "l1")).check(0);
    }

    @Test
    public void testMatchMapFieldWithAlias3() {
        filter(match("string_map$k1", "k1")).check(0);
    }

    @Test
    public void testMatchMapFieldWithAlias4() {
        filter(match("string_map$k1", "v1")).check(2);
    }
}
