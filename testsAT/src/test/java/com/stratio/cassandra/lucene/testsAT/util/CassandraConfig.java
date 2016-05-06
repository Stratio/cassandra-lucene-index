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

/**
 * Testing global variables.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class CassandraConfig {

    static final String HOST = getString("host", "127.0.0.1");
    static final int REPLICATION = getInt("replication", 1);
    static final ConsistencyLevel CONSISTENCY = ConsistencyLevel.valueOf(getString("consistency", "QUORUM"));
    static final int FETCH = getInt("fetch", 100);
    static final int THREADS = getInt("threads", 0);
    static final int REFRESH = getInt("refresh", 1);
    static final int WAIT_FOR_INDEXING = getInt("wait_for_indexing", 2);
    static final String TABLE = getString("table", "test_table");
    static final String INDEX = getString("index", "test_table_idx");
    static final int LIMIT = getInt("limit", 10000); // Top-k

    private static String getString(String key, String def) {
        return System.getProperty("it." + key, def);
    }

    private static int getInt(String key, Integer def) {
        return Integer.parseInt(getString(key, def.toString()));
    }
}
