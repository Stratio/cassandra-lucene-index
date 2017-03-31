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

import com.datastax.driver.core.*;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.monitoring.CassandraJMXClient;
import com.stratio.cassandra.lucene.testsAT.util.monitoring.CassandraJolokiaClient;
import com.stratio.cassandra.lucene.testsAT.util.monitoring.CassandraMonitoringClient;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.stratio.cassandra.lucene.testsAT.util.CassandraConfig.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class CassandraConnection {

    private static final Logger logger = BaseIT.logger;

    private static Cluster cluster;
    private static Session session;
    private static List<CassandraMonitoringClient> jmxClients;

    public static void connect() throws InterruptedException {
        if (cluster == null) {
            connect(CONNECTION_RETRIES, CONNECTION_DELAY);
        }
    }

    private static void connect(int connectionRetries, int conectionDelay) throws InterruptedException {
        if (connectionRetries > 0) {
            try {
                logger.debug("Connecting to: " + HOST);
                cluster = establishConnection();
            } catch (Exception runtimeException) {
                logger.debug("Error connecting to : " + HOST + " with error: "+ runtimeException.getLocalizedMessage());
                logger.debug("Waiting connection delay: "+ CONNECTION_DELAY+" secs");
                Thread.sleep(CONNECTION_DELAY * 1000);
                connect(connectionRetries - 1, conectionDelay);
            }
        } else {
            throw new RuntimeException(String.format("Error connecting to cassandra clsuter, in %d connections", CONNECTION_RETRIES));
        }
    }

    private static Cluster establishConnection() throws Exception {
        cluster = Cluster.builder().addContactPoint(HOST).build();

        cluster.getConfiguration().getQueryOptions().setConsistencyLevel(CONSISTENCY).setFetchSize(FETCH);
        cluster.getConfiguration()
               .getSocketOptions()
               .setReadTimeoutMillis(60000)
               .setConnectTimeoutMillis(100000);

        session = cluster.connect();
        jmxClients = new ArrayList<>(MONITOR_SERVICES_URL.length);
        if (Objects.equals(MONITOR_SERVICE, "jmx")) {
            for (String service : MONITOR_SERVICES_URL) {
                jmxClients.add(new CassandraJMXClient(service).connect());
            }
        } else if (Objects.equals(MONITOR_SERVICE, "jolokia")) {
            for (String service : MONITOR_SERVICES_URL) {
                jmxClients.add(new CassandraJolokiaClient(service).connect());
            }
        }
        return cluster;
    }

    public static void disconnect() {
        session.close();
        cluster.close();
        jmxClients.forEach(CassandraMonitoringClient::disconnect);
    }

    public static synchronized ResultSet execute(Statement statement) {
        logger.debug("CQL: {}", statement);
        return session.execute(statement);
    }

    static PreparedStatement prepare(String query) {
        return session.prepare(query);
    }

    public static <T> List<T> getJMXAttribute(String bean, String attribute) {
        try {
            List<Object> out = new ArrayList<>(jmxClients.size());
            for (CassandraMonitoringClient client : jmxClients) {
                out.add(client.read(bean, attribute));
            }
            return (List<T>) out;
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format("Error while reading JMX attribute %s.%s", bean, attribute), e);
        }
    }

    static void invokeJMXMethod(String bean, String operation, Object[] params, String[] signature) {
        try {
            for (CassandraMonitoringClient client : jmxClients) {
                client.invoke(bean, operation, params, signature);
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Error while invoking JMX method " + operation, e);
        }
    }
}
