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

package com.stratio.cassandra.lucene.testsAT.util;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Testing global variables.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraConfig {

    public static final String HOST = getString("host", "127.0.0.1");
    public static final int REPLICATION = getInt("replication", 1);
    public static final ConsistencyLevel CONSISTENCY = ConsistencyLevel.valueOf(getString("consistency", "QUORUM"));
    public static final int FETCH = getInt("fetch", 2);
    public static final int THREADS = getInt("threads", 0);
    public static final int REFRESH = getInt("refresh", 1);
    public static final int WAIT_FOR_INDEXING = getInt("wait_for_indexing", 2);
    public static final String TABLE = getString("table", "test_table");
    public static final String INDEX = getString("index", "test_table_idx");
    public static final int LIMIT = getInt("limit", 10000);
    public static final int TOKEN_RANGE_CACHE_SIZE = getInt("token_range_cache_size", 16);
    public static final int SEARCH_CACHE_SIZE = getInt("search_cache_size", 16);

    private static String getString(String key, String def) {
        return System.getProperty("it." + key, def);
    }

    private static int getInt(String key, Integer def) {
        return Integer.parseInt(getString(key, def.toString()));
    }
}
