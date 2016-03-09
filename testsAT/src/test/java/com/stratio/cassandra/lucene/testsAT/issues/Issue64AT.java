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

package com.stratio.cassandra.lucene.testsAT.issues;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue64AT extends BaseAT {

    @Test
    public void test() {

        CassandraUtils cassandraUtils = builder("issue_64").withTable("flights")
                                                           .withIndex("flights_index")
                                                           .withPartitionKey("id")
                                                           .withColumn("id", "uuid", null)
                                                           .withColumn("arrival_aerodrome", "text", stringMapper())
                                                           .withColumn("assigned_squawk", "text", stringMapper())
                                                           .withColumn("departure_aerodrome", "text", stringMapper())
                                                           .withColumn("departure_time",
                                                                       "timestamp",
                                                                       dateMapper().pattern("yyyy-MM-dd HH:mm:ss"))
                                                           .withColumn("arrival_time",
                                                                       "timestamp",
                                                                       dateMapper().pattern("yyyy-MM-dd HH:mm:ss"))
                                                           .withColumn("flight_status", "text", stringMapper())
                                                           .withColumn("registration", "text", stringMapper())
                                                           .withColumn("target_address", "text", stringMapper())
                                                           .withColumn("target_identification", "text", stringMapper())
                                                           .withColumn("lucene", "text")
                                                           .withMapper("operation_duration",
                                                                       dateRangeMapper("departure_time", "arrival_time")
                                                                               .pattern("yyyy-MM-dd HH:mm:ss"))
                                                           .build()
                                                           .createKeyspace()
                                                           .createTable()
                                                           .createIndex()
                                                           .refresh();
        cassandraUtils.filter(dateRange("operation_duration").from("2014-01-01 00:00:00")
                                                             .to("2014-12-31 23:59:59")
                                                             .operation("intersects")).get();
        cassandraUtils.dropTable().dropKeyspace();
    }
}
