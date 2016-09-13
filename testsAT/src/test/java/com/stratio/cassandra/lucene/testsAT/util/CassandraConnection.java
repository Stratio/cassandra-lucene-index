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

import com.datastax.driver.core.*;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import org.slf4j.Logger;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraConnection {

    protected static final Logger logger = BaseAT.logger;

    private static CassandraServer cassandraServer;
    public static Cluster cluster;
    public static Session session;

    public static void start() {
        //startEmbeddedServer();
        connect();
    }

    public static void stop() {
        disconnect();
        //stopEmbeddedServer();
    }

    private static void startEmbeddedServer() {
        if (EMBEDDED) {
            try {
                cassandraServer = new CassandraServer();
                cassandraServer.setup();
            } catch (Exception e) {
                throw new RuntimeException("Error while starting Cassandra server", e);
            }
        }
    }

    private static void stopEmbeddedServer() {
        if (cassandraServer != null) {
            try {
                cassandraServer.teardown();
            } catch (Exception e) {
                throw new RuntimeException("Error while stopping Cassandra server", e);
            }
        }
    }

    private static void connect() {
        try {
            cluster = Cluster.builder().addContactPoint(HOST).build();
            cluster.getConfiguration().getQueryOptions().setConsistencyLevel(CONSISTENCY);
            cluster.getConfiguration().getQueryOptions().setFetchSize(FETCH);
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000);
            session = cluster.connect();
        } catch (Exception e) {
            throw new RuntimeException("Error while connecting to Cassandra server", e);
        }
    }

    private static void disconnect() {
        session.close();
        cluster.close();
    }

    public static ResultSet execute(Statement statement) {
        logger.debug("CQL: " + statement);
        return session.execute(statement);
    }
}
