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

import com.datastax.driver.core.LocalDate;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test sorting by DATE(Date without time) column (<a href="https://github.com/Stratio/cassandra-lucene-index/issues/313">issue 313</a>)
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue313IT extends BaseIT {

    @Test
    public void test() {
        builder("issue_313").withTable("test")
                            .withIndexName("idx")
                            .withColumn("my_id", "bigint")
                            .withColumn("my_date", "date")
                            .withIndexColumn("lucene")
                            .withPartitionKey("my_id")
                            .withMapper("my_date", dateMapper().pattern("yyyy-MM-dd"))
                            .build()
                            .createKeyspace()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{1L, LocalDate.fromYearMonthDay(2015, 11, 6)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{2L, LocalDate.fromYearMonthDay(2015, 11, 7)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{3L, LocalDate.fromYearMonthDay(2015, 11, 8)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{4L, LocalDate.fromYearMonthDay(2015, 11, 9)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{5L, LocalDate.fromYearMonthDay(2015, 11, 10)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{6L, LocalDate.fromYearMonthDay(2015, 11, 11)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{7L, LocalDate.fromYearMonthDay(2015, 11, 12)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{8L, LocalDate.fromYearMonthDay(2015, 11, 13)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{9L, LocalDate.fromYearMonthDay(2015, 11, 14)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{10L, LocalDate.fromYearMonthDay(2015, 11, 15)})
                            .refresh()
                            .filter(match("my_date", "2015-11-10")).check(1)
                            .filter(match("my_date", "2015-11-10")).sort(field("my_date").reverse(false)).check(1)
                            .filter(match("my_date", "2015-11-10")).sort(field("my_date").reverse(true)).check(1)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(false)).check(10)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(true)).check(10)
                            .filter(range("my_date").lower("2015-11-07").upper("2015-11-10").includeLower(true).includeUpper(true)).sort(field("my_date").reverse(false)).check(4)
                            .filter(range("my_date").lower("2015-11-07").upper("2015-11-10").includeLower(true).includeUpper(true)).sort(field("my_date").reverse(true)).check(4)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(false)).checkOrderedColumns("my_id", 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(true)).checkOrderedColumns("my_id", 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L)
                            .dropKeyspace();
    }
}
