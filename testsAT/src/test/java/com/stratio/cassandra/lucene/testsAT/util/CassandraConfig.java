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
package com.stratio.cassandra.lucene.testsAT.util;

import com.datastax.driver.core.ConsistencyLevel;
import com.stratio.cassandra.lucene.builder.index.Partitioner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Testing global variables.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class CassandraConfig {

    static final String HOST = getIP("host", "127.0.0.1");
    static final String JMX_PORT = getString("jmx_port", "7199");
    static final String MONITOR_SERVICE = getString("monitor_service","jmx");// jmx or jolokia
    static final String[] MONITOR_SERVICES_URL = getStringArray("monitor_services_url", HOST + ":" + JMX_PORT);
    // static final String[] MONITOR_SERVICES_URL = getStringArray("jmx_services", "127.0.0.1:7100,127.0.0.1:7200,127.0.0.1:7300");
    static final int REPLICATION = getInt("replication", 1);
    static final ConsistencyLevel CONSISTENCY = ConsistencyLevel.valueOf(getString("consistency", "QUORUM"));
    static final int FETCH = getInt("fetch", 100);
    static final int THREADS = getInt("threads", Runtime.getRuntime().availableProcessors());
    static final int REFRESH = getInt("refresh", 1);
    static final String TABLE = getString("table", "test_table");
    static final String INDEX = getString("index", "test_table_idx");
    static final String COLUMN = getString("column", "lucene");
    static final boolean USE_NEW_QUERY_SYNTAX = getBool("use_new_query_syntax", true);
    static final int LIMIT = getInt("limit", 10000);
    static final Partitioner PARTITIONER = new Partitioner.None();

    static {
        assert COLUMN != null || USE_NEW_QUERY_SYNTAX;
    }

    private static String getString(String key, String def) {
        return System.getProperty("it." + key, def);
    }

    private static String[] getStringArray(String key, String def) {
        return getString(key, def).split(",");
    }

    private static int getInt(String key, Integer def) {
        return Integer.parseInt(getString(key, def.toString()));
    }

    private static Boolean getBool(String key, Boolean def) {
        return Boolean.parseBoolean(getString(key, def.toString()));
    }

    private static String getIP(String key, String def) {
        String value = System.getProperty("it." + key, def);
        Pattern pattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        Matcher m = pattern.matcher(value);
        if (m.find()) { // It is an IP
            return value;
        } else {
            InetAddress domainAddress;
            try {
                domainAddress = InetAddress.getByName(value);
            } catch (UnknownHostException e) {
                return def;
            }
            return domainAddress.getHostAddress();
        }
    }
}
