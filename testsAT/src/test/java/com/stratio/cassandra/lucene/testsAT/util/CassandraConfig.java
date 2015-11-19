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
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraConfig {

    static final boolean EMBEDDED = Boolean.parseBoolean(get("embedded", "true"));
    static final String HOST = get("host", "127.0.0.1");
    static final int REPLICATION = Integer.valueOf(get("replication", "1"));
    static final ConsistencyLevel CONSISTENCY = ConsistencyLevel.valueOf(get("consistency", "QUORUM"));
    static final int FETCH = Integer.parseInt(get("fetch", "100"));
    static final int THREADS = Integer.parseInt(get("threads", "0"));
    static final int REFRESH = Integer.parseInt(get("refresh", "1"));
    static final int WAIT_FOR_INDEXING = Integer.parseInt(get("wait_for_indexing", "1"));
    static final int WAIT_FOR_SERVER = Integer.parseInt(get("wait_for_server", "30"));
    static final String TABLE = get("table", "test_table");
    static final String INDEX = get("index", "test_table_idx");
    static final String COLUMN = get("column", "lucene");

    private static String get(String key, String def) {
        return System.getProperty("it." + key, def);
    }
}
