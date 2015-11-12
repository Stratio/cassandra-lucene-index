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

package com.stratio.cassandra.lucene.suite;

import com.stratio.cassandra.lucene.util.CassandraServer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.io.IOException;

@RunWith(Suite.class)
@SuiteClasses({SearchSuite.class,
               DeletionSuite.class,
               IndexesSuite.class,
               VariaSuite.class,
               StoriesSuite.class,
               BitemporalSuite.class,
               IssuesSuite.class})
public class AllSuite {

    private static CassandraServer cassandraServer;

    @BeforeClass
    public static void before() {
        if (cassandraServer == null) {
            try {
                cassandraServer = new CassandraServer();
                cassandraServer.setup();
            } catch (IOException | ConfigurationException | TTransportException | InterruptedException e) {
                throw new RuntimeException("Error while starting Cassandra server", e);
            }
        }
    }

    @AfterClass
    public static void after() {
        if (cassandraServer != null) {
            try {
                CassandraServer.cleanup();
                CassandraServer.teardown();
            } catch (IOException e) {
                throw new RuntimeException("Error while stopping Cassandra server", e);
            }
        }
    }

}
