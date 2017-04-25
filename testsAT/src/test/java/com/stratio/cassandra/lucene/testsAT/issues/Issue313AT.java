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
import com.stratio.cassandra.lucene.testsAT.BaseAT;
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
public class Issue313AT extends BaseAT {

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
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{1, LocalDate.fromYearMonthDay(2015, 11, 6)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{2, LocalDate.fromYearMonthDay(2015, 11, 7)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{3, LocalDate.fromYearMonthDay(2015, 11, 8)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{4, LocalDate.fromYearMonthDay(2015, 11, 9)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{5, LocalDate.fromYearMonthDay(2015, 11, 10)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{6, LocalDate.fromYearMonthDay(2015, 11, 11)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{7, LocalDate.fromYearMonthDay(2015, 11, 12)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{8, LocalDate.fromYearMonthDay(2015, 11, 13)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{9, LocalDate.fromYearMonthDay(2015, 11, 14)})
                            .insert(new String[]{"my_id", "my_date"}, new Object[]{10, LocalDate.fromYearMonthDay(2015, 11, 15)})
                            .refresh()
                            .filter(match("my_date", "2015-11-10")).check(1)
                            .filter(match("my_date", "2015-11-10")).sort(field("my_date").reverse(false)).check(1)
                            .filter(match("my_date", "2015-11-10")).sort(field("my_date").reverse(true)).check(1)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(false)).check(10)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(true)).check(10)
                            .filter(range("my_date").lower("2015-11-07").upper("2015-11-10").includeLower(true).includeUpper(true)).sort(field("my_date").reverse(false)).check(4)
                            .filter(range("my_date").lower("2015-11-07").upper("2015-11-10").includeLower(true).includeUpper(true)).sort(field("my_date").reverse(true)).check(4)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(false)).checkOrderedLongColumns("my_id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                            .filter(range("my_date").lower("2015-11-05").upper("2015-11-16")).sort(field("my_date").reverse(true)).checkOrderedLongColumns("my_id", 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
                            .dropKeyspace();
    }
}
