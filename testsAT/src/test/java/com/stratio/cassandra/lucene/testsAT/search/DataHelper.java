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

import java.util.LinkedHashMap;
import java.util.Map;

public final class DataHelper {

    public static final Map<String, String> data1;
    public static final Map<String, String> data2;
    public static final Map<String, String> data3;
    public static final Map<String, String> data4;
    public static final Map<String, String> data5;
    public static final Map<String, String> data6;
    public static final Map<String, String> data7;

    static {
        data1 = new LinkedHashMap<>();
        data1.put("ascii_1", "'frase tipo ascii'");
        data1.put("bigint_1", "1000000000000000");
        data1.put("blob_1", "0x3E0A16");
        data1.put("boolean_1", "true");
        data1.put("decimal_1", "1000000000.0");
        data1.put("date_1", String.valueOf(System.currentTimeMillis()));
        data1.put("double_1", "1.0");
        data1.put("float_1", "1.0");
        data1.put("integer_1", "-1");
        data1.put("inet_1", "'127.0.0.1'");
        data1.put("text_1", "'Frase con espacios articulos y las palabras suficientes'");
        data1.put("varchar_1", "'frase sencilla con espacios'");
        data1.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data1.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data1.put("list_1", "['l1','l2']");
        data1.put("set_1", "{'s1','s2'}");
        data1.put("map_1", "{'k1':'v1','k2':'v2'}");

        data2 = new LinkedHashMap<>();
        data2.put("ascii_1", "'frasetipoasciisinespacios'");
        data2.put("bigint_1", "2000000000000000");
        data2.put("blob_1", "0x3E0A16");
        data2.put("boolean_1", "false");
        data2.put("decimal_1", "2000000000.0");
        data2.put("date_1", String.valueOf(System.currentTimeMillis()));
        data2.put("double_1", "2.0");
        data2.put("float_1", "2.0");
        data2.put("integer_1", "-2");
        data2.put("inet_1", "'127.0.01.01'");
        data2.put("text_1", "'Frasesinespaciosconarticulosylaspalabrassuficientes'");
        data2.put("varchar_1", "'frasesencillasinespacios'");
        data2.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        data2.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591712");
        data2.put("list_1", "['l1','l3']");
        data2.put("set_1", "{'s1','s3'}");
        data2.put("map_1", "{'k1':'v1','k3':'v3'}");

        data3 = new LinkedHashMap<>();
        data3.put("ascii_1", "'frasetipoasciisinespaciosperomaslarga'");
        data3.put("bigint_1", "3000000000000000");
        data3.put("blob_1", "0x3E0A15");
        data3.put("boolean_1", "true");
        data3.put("decimal_1", "3000000000.0");
        data3.put("date_1", String.valueOf(System.currentTimeMillis()));
        data3.put("double_1", "3.0");
        data3.put("float_1", "3.0");
        data3.put("integer_1", "-3");
        data3.put("inet_1", "'127.1.1.1'");
        data3.put("text_1", "'Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga'");
        data3.put("varchar_1", "'frasesencillasinespaciosperomaslarga'");
        data3.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data3.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data3.put("list_1", "['l2','l3']");
        data3.put("set_1", "{'s2','s3'}");
        data3.put("map_1", "{'k2':'v2','k3':'v3'}");

        data4 = new LinkedHashMap<>();
        data4.put("ascii_1", "'frasetipoasciisinespaciosperomaslargaaaa'");
        data4.put("bigint_1", "3000000000000000");
        data4.put("blob_1", "0x3E0A16");
        data4.put("boolean_1", "true");
        data4.put("decimal_1", "3000000000.0");
        data4.put("date_1", String.valueOf(System.currentTimeMillis()));
        data4.put("double_1", "3.0");
        data4.put("float_1", "3.0");
        data4.put("integer_1", "-4");
        data4.put("inet_1", "'127.1.1.1'");
        data4.put("text_1", "'Frasesinespaciosconarticulosylaspalabrassuficientesperomaslargaaaaa  '");
        data4.put("varchar_1", "'frasesencillasinespaciosperomaslarga'");
        data4.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data4.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data4.put("list_1", "['l2','l3']");
        data4.put("set_1", "{'s2','s3'}");
        data4.put("map_1", "{'k2':'v2','k3':'v3'}");

        data5 = new LinkedHashMap<>(); ///
        data5.put("ascii_1", "'prase tipo ascii'");
        data5.put("bigint_1", "3000000000000000");
        data5.put("blob_1", "0x3E0A16");
        data5.put("boolean_1", "true");
        data5.put("decimal_1", "3000000000.0");
        data5.put("date_1", String.valueOf(System.currentTimeMillis()));
        data5.put("double_1", "3.0");
        data5.put("float_1", "3.0");
        data5.put("integer_1", "-5");
        data5.put("inet_1", "'192.168.0.1'");
        data5.put("text_1", "'Prasesinespaciosconarticulosylaspalabrassuficientes'");
        data5.put("varchar_1", "'prasesencillasinespaciosperomaslarga'");
        data5.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data5.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data5.put("list_1", "['l2','l3']");
        data5.put("set_1", "{'s2','s3'}");
        data5.put("map_1", "{'k2':'v2','k3':'v3'}");

        data6 = new LinkedHashMap<>();
        data6.put("ascii_1", "'frase frase y una frase'");
        data6.put("bigint_1", "3000000000000000");
        data6.put("blob_1", "0x3E0A16");
        data6.put("boolean_1", "true");
        data6.put("decimal_1", "3000000000.0");
        data6.put("date_1", String.valueOf(System.currentTimeMillis()));
        data6.put("double_1", "6.0");
        data6.put("float_1", "3.0");
        data6.put("integer_1", "-6");
        data6.put("inet_1", "'127.1.1.1'");
        data6.put("text_1", "'frase y frase  '");
        data6.put("varchar_1", "'frasesencillasinespaciosperomaslarga'");
        data6.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data6.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data6.put("list_1", "['l2','l3']");
        data6.put("set_1", "{'s2','s3'}");
        data6.put("map_1", "{'k2':'v2','k3':'v3'}");

        data7 = new LinkedHashMap<>();
        data7.put("ascii_1", "'frase frase y una frase sin frase'");
        data7.put("bigint_1", "3000000000000000");
        data7.put("blob_1", "0x3E0A16");
        data7.put("boolean_1", "true");
        data7.put("decimal_1", "3000000000.0");
        data7.put("date_1", String.valueOf(System.currentTimeMillis()));
        data7.put("double_1", "7.0");
        data7.put("float_1", "3.0");
        data7.put("integer_1", "-7");
        data7.put("inet_1", "'127.1.1.1'");
        data7.put("text_1", "'frase, frase con frase y frase'");
        data7.put("varchar_1", "'prasesencillasinespaciosperomaslarga'");
        data7.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data7.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data7.put("list_1", "['l2','l3']");
        data7.put("set_1", "{'s2','s3'}");
        data7.put("map_1", "{'k2':'v2','k3':'v3'}");
    }
}
