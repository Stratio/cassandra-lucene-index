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
package com.stratio.cassandra.lucene.testsAT.issues;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.datastax.driver.core.LocalDate.fromYearMonthDay;
import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test parsing of data with CQL "date" type.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue158AT extends BaseAT {

    @Test
    public void testLocalDate() {
        builder("issue_158")
                .withTable("test")
                .withIndexName("idx")
                .withColumn("id", "text", stringMapper())
                .withColumn("dob", "date", dateMapper().pattern("yyyy-MM-dd"))
                .withColumn("start", "date", null)
                .withColumn("stop", "date", null)
                .withColumn("vt_to", "date", null)
                .withColumn("vt_from", "date", null)
                .withColumn("tt_to", "date", null)
                .withColumn("tt_from", "date", null)
                .withPartitionKey("id")
                .withMapper("duration", dateRangeMapper("start", "stop").pattern("yyyy/MM/dd"))
                .withMapper("bitemp", bitemporalMapper("vt_to", "vt_from", "tt_to", "tt_from").pattern("yyyy:MM:dd"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(new String[]{"id", "dob", "start", "stop", "vt_to", "vt_from", "tt_to", "tt_from"},
                        new Object[]{"unique_id",
                                     fromYearMonthDay(2015, 10, 10),
                                     fromYearMonthDay(2015, 10, 10),
                                     fromYearMonthDay(2015, 10, 20),
                                     fromYearMonthDay(2015, 10, 10),
                                     fromYearMonthDay(2015, 10, 20),
                                     fromYearMonthDay(2015, 10, 20),
                                     fromYearMonthDay(2015, 10, 30)})
                .refresh()
                .filter(match("dob", "2015-10-10")).check(1)
                .filter(dateRange("duration").from("2015/10/10").to("2015/10/15")).check(1)
                .filter(dateRange("duration").to("2015/10/10")).check(1)
                .filter(dateRange("duration").to("2015/10/9")).check(0)
                .filter(dateRange("duration").from("2015/10/20")).check(1)
                .filter(dateRange("duration").from("2015/10/21")).check(0)
                .filter(bitemporal("bitemp").vtFrom("2015:11:10").vtTo("2015:11:20")).check(0)
                .filter(bitemporal("bitemp").ttFrom("2015:11:20").ttTo("2015:11:30")).check(0)
                .filter(bitemporal("bitemp").vtFrom("2015:10:10")
                                            .vtTo("2015:10:20")
                                            .ttFrom("2015:10:20")
                                            .ttTo("2015:10:30")).check(1)
                .dropKeyspace();
    }
}
