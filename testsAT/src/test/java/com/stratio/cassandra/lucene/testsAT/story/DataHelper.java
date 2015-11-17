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

package com.stratio.cassandra.lucene.testsAT.story;

import java.util.LinkedHashMap;
import java.util.Map;

public final class DataHelper {

    protected static final Map<String, String> data1;
    protected static final Map<String, String> data2;
    protected static final Map<String, String> data3;
    protected static final Map<String, String> data4;
    protected static final Map<String, String> data5;
    protected static final Map<String, String> data6;
    protected static final Map<String, String> data7;
    protected static final Map<String, String> data8;
    protected static final Map<String, String> data9;
    protected static final Map<String, String> data10;
    protected static final Map<String, String> data11;
    protected static final Map<String, String> data12;
    protected static final Map<String, String> data13;
    protected static final Map<String, String> data14;
    protected static final Map<String, String> data15;
    protected static final Map<String, String> data16;
    protected static final Map<String, String> data17;
    protected static final Map<String, String> data18;
    protected static final Map<String, String> data19;
    protected static final Map<String, String> data20;

    static {
        data1 = new LinkedHashMap<>();
        data1.put("ascii_1", "'ascii'");
        data1.put("bigint_1", "1000000000000000");
        data1.put("blob_1", "0x3E0A16");
        data1.put("boolean_1", "true");
        data1.put("decimal_1", "1000000000.0");
        data1.put("date_1", String.valueOf(System.currentTimeMillis()));
        data1.put("double_1", "1.0");
        data1.put("float_1", "1.0");
        data1.put("integer_1", "1");
        data1.put("inet_1", "'127.0.0.1'");
        data1.put("text_1", "'text'");
        data1.put("varchar_1", "'varchar'");
        data1.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data1.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data1.put("list_1", "['l1','l2']");
        data1.put("set_1", "{'s1','s2'}");
        data1.put("map_1", "{'k1':'v1','k2':'v2'}");

        data2 = new LinkedHashMap<>();
        data2.put("ascii_1", "'ascii'");
        data2.put("bigint_1", "2000000000000000");
        data2.put("blob_1", "0x3E0A16");
        data2.put("boolean_1", "false");
        data2.put("decimal_1", "2000000000.0");
        data2.put("date_1", String.valueOf(System.currentTimeMillis()));
        data2.put("double_1", "2.0");
        data2.put("float_1", "2.0");
        data2.put("integer_1", "2");
        data2.put("inet_1", "'127.0.01.01'");
        data2.put("text_1", "'text'");
        data2.put("varchar_1", "'varchar'");
        data2.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        data2.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591712");
        data2.put("list_1", "['l1','l3']");
        data2.put("set_1", "{'s1','s3'}");
        data2.put("map_1", "{'k1':'v1','k3':'v3'}");

        data3 = new LinkedHashMap<>();
        data3.put("ascii_1", "'ascii'");
        data3.put("bigint_1", "3000000000000000");
        data3.put("blob_1", "0x3E0A15");
        data3.put("boolean_1", "true");
        data3.put("decimal_1", "3000000000.0");
        data3.put("date_1", String.valueOf(System.currentTimeMillis()));
        data3.put("double_1", "2.0");
        data3.put("float_1", "3.0");
        data3.put("integer_1", "3");
        data3.put("inet_1", "'127.1.1.1'");
        data3.put("text_1", "'text'");
        data3.put("varchar_1", "'varchar'");
        data3.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data3.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data3.put("list_1", "['l2','l3']");
        data3.put("set_1", "{'s2','s3'}");
        data3.put("map_1", "{'k2':'v2','k3':'v3'}");

        data4 = new LinkedHashMap<>();
        data4.put("ascii_1", "'ascii'");
        data4.put("bigint_1", "3000000000000000");
        data4.put("blob_1", "0x3E0A16");
        data4.put("boolean_1", "true");
        data4.put("decimal_1", "3000000000.0");
        data4.put("date_1", String.valueOf(System.currentTimeMillis()));
        data4.put("double_1", "2.0");
        data4.put("float_1", "3.0");
        data4.put("integer_1", "4");
        data4.put("inet_1", "'127.1.1.1'");
        data4.put("text_1", "'text'");
        data4.put("varchar_1", "'varchar'");
        data4.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data4.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data4.put("list_1", "['l2','l3']");
        data4.put("set_1", "{'s2','s3'}");
        data4.put("map_1", "{'k2':'v2','k3':'v3'}");

        data5 = new LinkedHashMap<>();
        data5.put("ascii_1", "'ascii'");
        data5.put("bigint_1", "3000000000000000");
        data5.put("blob_1", "0x3E0A16");
        data5.put("boolean_1", "true");
        data5.put("decimal_1", "3000000000.0");
        data5.put("date_1", String.valueOf(System.currentTimeMillis()));
        data5.put("double_1", "1.0");
        data5.put("float_1", "3.0");
        data5.put("integer_1", "5");
        data5.put("inet_1", "'192.168.0.1'");
        data5.put("text_1", "'text'");
        data5.put("varchar_1", "'varchar'");
        data5.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data5.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data5.put("list_1", "['l2','l3']");
        data5.put("set_1", "{'s2','s3'}");
        data5.put("map_1", "{'k2':'v2','k3':'v3'}");

        data6 = new LinkedHashMap<>();
        data6.put("ascii_1", "'ascii_bis'");
        data6.put("bigint_1", "1000000000000000");
        data6.put("blob_1", "0x3E0A16");
        data6.put("boolean_1", "true");
        data6.put("decimal_1", "1000000000.0");
        data6.put("date_1", String.valueOf(System.currentTimeMillis()));
        data6.put("double_1", "1.0");
        data6.put("float_1", "1.0");
        data6.put("integer_1", "1");
        data6.put("inet_1", "'127.0.0.1'");
        data6.put("text_1", "'text'");
        data6.put("varchar_1", "'varchar'");
        data6.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data6.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data6.put("list_1", "['l1','l2']");
        data6.put("set_1", "{'s1','s2'}");
        data6.put("map_1", "{'k1':'v1','k2':'v2'}");

        data7 = new LinkedHashMap<>();
        data7.put("ascii_1", "'ascii_bis'");
        data7.put("bigint_1", "2000000000000000");
        data7.put("blob_1", "0x3E0A16");
        data7.put("boolean_1", "false");
        data7.put("decimal_1", "2000000000.0");
        data7.put("date_1", String.valueOf(System.currentTimeMillis()));
        data7.put("double_1", "2.0");
        data7.put("float_1", "2.0");
        data7.put("integer_1", "2");
        data7.put("inet_1", "'127.0.01.01'");
        data7.put("text_1", "'text'");
        data7.put("varchar_1", "'varchar'");
        data7.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        data7.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591712");
        data7.put("list_1", "['l1','l3']");
        data7.put("set_1", "{'s1','s3'}");
        data7.put("map_1", "{'k1':'v1','k3':'v3'}");

        data8 = new LinkedHashMap<>();

        data8.put("ascii_1", "'ascii_bis'");
        data8.put("bigint_1", "3000000000000000");
        data8.put("blob_1", "0x3E0A15");
        data8.put("boolean_1", "true");
        data8.put("decimal_1", "3000000000.0");
        data8.put("date_1", String.valueOf(System.currentTimeMillis()));
        data8.put("double_1", "2.0");
        data8.put("float_1", "3.0");
        data8.put("integer_1", "3");
        data8.put("inet_1", "'127.1.1.1'");
        data8.put("text_1", "'text'");
        data8.put("varchar_1", "'varchar'");
        data8.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data8.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data8.put("list_1", "['l2','l3']");
        data8.put("set_1", "{'s2','s3'}");
        data8.put("map_1", "{'k2':'v2','k3':'v3'}");

        data9 = new LinkedHashMap<>();
        data9.put("ascii_1", "'ascii_bis'");
        data9.put("bigint_1", "3000000000000000");
        data9.put("blob_1", "0x3E0A16");
        data9.put("boolean_1", "true");
        data9.put("decimal_1", "3000000000.0");
        data9.put("date_1", String.valueOf(System.currentTimeMillis()));
        data9.put("double_1", "2.0");
        data9.put("float_1", "3.0");
        data9.put("integer_1", "4");
        data9.put("inet_1", "'127.1.1.1'");
        data9.put("text_1", "'text'");
        data9.put("varchar_1", "'varchar'");
        data9.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data9.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data9.put("list_1", "['l2','l3']");
        data9.put("set_1", "{'s2','s3'}");
        data9.put("map_1", "{'k2':'v2','k3':'v3'}");

        data10 = new LinkedHashMap<>();
        data10.put("ascii_1", "'ascii_bis'");
        data10.put("bigint_1", "3000000000000000");
        data10.put("blob_1", "0x3E0A16");
        data10.put("boolean_1", "true");
        data10.put("decimal_1", "3000000000.0");
        data10.put("date_1", String.valueOf(System.currentTimeMillis()));
        data10.put("double_1", "1.0");
        data10.put("float_1", "3.0");
        data10.put("integer_1", "5");
        data10.put("inet_1", "'192.168.0.1'");
        data10.put("text_1", "'text'");
        data10.put("varchar_1", "'varchar'");
        data10.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data10.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data10.put("list_1", "['l2','l3']");
        data10.put("set_1", "{'s2','s3'}");
        data10.put("map_1", "{'k2':'v2','k3':'v3'}");

        data11 = new LinkedHashMap<>();
        data11.put("ascii_1", "'ascii'");
        data11.put("bigint_1", "1000000000000000");
        data11.put("blob_1", "0x3E0A16");
        data11.put("boolean_1", "true");
        data11.put("decimal_1", "1000000000.0");
        data11.put("date_1", String.valueOf(System.currentTimeMillis()));
        data11.put("double_1", "2.0");
        data11.put("float_1", "1.0");
        data11.put("integer_1", "1");
        data11.put("inet_1", "'127.0.0.1'");
        data11.put("text_1", "'text'");
        data11.put("varchar_1", "'varchar'");
        data11.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data11.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data11.put("list_1", "['l1','l2']");
        data11.put("set_1", "{'s1','s2'}");
        data11.put("map_1", "{'k1':'v1','k2':'v2'}");

        data12 = new LinkedHashMap<>();
        data12.put("ascii_1", "'ascii'");
        data12.put("bigint_1", "2000000000000000");
        data12.put("blob_1", "0x3E0A16");
        data12.put("boolean_1", "false");
        data12.put("decimal_1", "2000000000.0");
        data12.put("date_1", String.valueOf(System.currentTimeMillis()));
        data12.put("double_1", "1.0");
        data12.put("float_1", "2.0");
        data12.put("integer_1", "2");
        data12.put("inet_1", "'127.0.01.01'");
        data12.put("text_1", "'text'");
        data12.put("varchar_1", "'varchar'");
        data12.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        data12.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591712");
        data12.put("list_1", "['l1','l3']");
        data12.put("set_1", "{'s1','s3'}");
        data12.put("map_1", "{'k1':'v1','k3':'v3'}");

        data13 = new LinkedHashMap<>();

        data13.put("ascii_1", "'ascii'");
        data13.put("bigint_1", "3000000000000000");
        data13.put("blob_1", "0x3E0A15");
        data13.put("boolean_1", "true");
        data13.put("decimal_1", "3000000000.0");
        data13.put("date_1", String.valueOf(System.currentTimeMillis()));
        data13.put("double_1", "1.0");
        data13.put("float_1", "3.0");
        data13.put("integer_1", "3");
        data13.put("inet_1", "'127.1.1.1'");
        data13.put("text_1", "'text'");
        data13.put("varchar_1", "'varchar'");
        data13.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data13.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data13.put("list_1", "['l2','l3']");
        data13.put("set_1", "{'s2','s3'}");
        data13.put("map_1", "{'k2':'v2','k3':'v3'}");

        data14 = new LinkedHashMap<>();
        data14.put("ascii_1", "'ascii'");
        data14.put("bigint_1", "3000000000000000");
        data14.put("blob_1", "0x3E0A16");
        data14.put("boolean_1", "true");
        data14.put("decimal_1", "3000000000.0");
        data14.put("date_1", String.valueOf(System.currentTimeMillis()));
        data14.put("double_1", "1.0");
        data14.put("float_1", "3.0");
        data14.put("integer_1", "4");
        data14.put("inet_1", "'127.1.1.1'");
        data14.put("text_1", "'text'");
        data14.put("varchar_1", "'varchar'");
        data14.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data14.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data14.put("list_1", "['l2','l3']");
        data14.put("set_1", "{'s2','s3'}");
        data14.put("map_1", "{'k2':'v2','k3':'v3'}");

        data15 = new LinkedHashMap<>();
        data15.put("ascii_1", "'ascii'");
        data15.put("bigint_1", "3000000000000000");
        data15.put("blob_1", "0x3E0A16");
        data15.put("boolean_1", "true");
        data15.put("decimal_1", "3000000000.0");
        data15.put("date_1", String.valueOf(System.currentTimeMillis()));
        data15.put("double_1", "2.0");
        data15.put("float_1", "3.0");
        data15.put("integer_1", "5");
        data15.put("inet_1", "'192.168.0.1'");
        data15.put("text_1", "'text'");
        data15.put("varchar_1", "'varchar'");
        data15.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data15.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data15.put("list_1", "['l2','l3']");
        data15.put("set_1", "{'s2','s3'}");
        data15.put("map_1", "{'k2':'v2','k3':'v3'}");

        data16 = new LinkedHashMap<>();
        data16.put("ascii_1", "'ascii_bis'");
        data16.put("bigint_1", "1000000000000000");
        data16.put("blob_1", "0x3E0A16");
        data16.put("boolean_1", "true");
        data16.put("decimal_1", "1000000000.0");
        data16.put("date_1", String.valueOf(System.currentTimeMillis()));
        data16.put("double_1", "2.0");
        data16.put("float_1", "2.0");
        data16.put("integer_1", "1");
        data16.put("inet_1", "'127.0.0.1'");
        data16.put("text_1", "'text'");
        data16.put("varchar_1", "'varchar'");
        data16.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data16.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data16.put("list_1", "['l1','l2']");
        data16.put("set_1", "{'s1','s2'}");
        data16.put("map_1", "{'k1':'v1','k2':'v2'}");

        data17 = new LinkedHashMap<>();
        data17.put("ascii_1", "'ascii_bis'");
        data17.put("bigint_1", "2000000000000000");
        data17.put("blob_1", "0x3E0A16");
        data17.put("boolean_1", "false");
        data17.put("decimal_1", "2000000000.0");
        data17.put("date_1", String.valueOf(System.currentTimeMillis()));
        data17.put("double_1", "1.0");
        data17.put("float_1", "2.0");
        data17.put("integer_1", "2");
        data17.put("inet_1", "'127.0.01.01'");
        data17.put("text_1", "'text'");
        data17.put("varchar_1", "'varchar'");
        data17.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        data17.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591712");
        data17.put("list_1", "['l1','l3']");
        data17.put("set_1", "{'s1','s3'}");
        data17.put("map_1", "{'k1':'v1','k3':'v3'}");

        data18 = new LinkedHashMap<>();
        data18.put("ascii_1", "'ascii_bis'");
        data18.put("bigint_1", "3000000000000000");
        data18.put("blob_1", "0x3E0A15");
        data18.put("boolean_1", "true");
        data18.put("decimal_1", "3000000000.0");
        data18.put("date_1", String.valueOf(System.currentTimeMillis()));
        data18.put("double_1", "1.0");
        data18.put("float_1", "3.0");
        data18.put("integer_1", "3");
        data18.put("inet_1", "'127.1.1.1'");
        data18.put("text_1", "'text'");
        data18.put("varchar_1", "'varchar'");
        data18.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data18.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data18.put("list_1", "['l2','l3']");
        data18.put("set_1", "{'s2','s3'}");
        data18.put("map_1", "{'k2':'v2','k3':'v3'}");

        data19 = new LinkedHashMap<>();
        data19.put("ascii_1", "'ascii_bis'");
        data19.put("bigint_1", "3000000000000000");
        data19.put("blob_1", "0x3E0A16");
        data19.put("boolean_1", "true");
        data19.put("decimal_1", "3000000000.0");
        data19.put("date_1", String.valueOf(System.currentTimeMillis()));
        data19.put("double_1", "1.0");
        data19.put("float_1", "3.0");
        data19.put("integer_1", "4");
        data19.put("inet_1", "'127.1.1.1'");
        data19.put("text_1", "'text'");
        data19.put("varchar_1", "'varchar'");
        data19.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data19.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data19.put("list_1", "['l2','l3']");
        data19.put("set_1", "{'s2','s3'}");
        data19.put("map_1", "{'k2':'v2','k3':'v3'}");

        data20 = new LinkedHashMap<>();
        data20.put("ascii_1", "'ascii_bis'");
        data20.put("bigint_1", "3000000000000000");
        data20.put("blob_1", "0x3E0A16");
        data20.put("boolean_1", "true");
        data20.put("decimal_1", "3000000000.0");
        data20.put("date_1", String.valueOf(System.currentTimeMillis()));
        data20.put("double_1", "2.0");
        data20.put("float_1", "3.0");
        data20.put("integer_1", "5");
        data20.put("inet_1", "'192.168.0.1'");
        data20.put("text_1", "'text'");
        data20.put("varchar_1", "'varchar'");
        data20.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        data20.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
        data20.put("list_1", "['l2','l3']");
        data20.put("set_1", "{'s2','s3'}");
        data20.put("map_1", "{'k2':'v2','k3':'v3'}");
    }
}
