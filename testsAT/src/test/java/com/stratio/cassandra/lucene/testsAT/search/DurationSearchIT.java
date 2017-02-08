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
package com.stratio.cassandra.lucene.testsAT.search;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * Tests for CQL duration search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class DurationSearchIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("duration_search")
                              .withPartitionKey("pk")
                              .withClusteringKey("ck")
                              .withColumn("pk", "int", null)
                              .withColumn("ck", "int", null)
                              .withColumn("duration", "duration", durationMapper().validated(true))
                              .withColumn("text", "text", durationMapper().validated(true))
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        insert(0, "1y2mo3w4d5h6m7s");
        insert(1, "1mo");
        insert(2, "1ns");
        insert(3, "2ns");
        insert(4, "59s");
        insert(5, "1m");
        insert(6, "60s");
        insert(7, "61s");
        insert(8, "1h1m");
        insert(9, "-2m");
        utils.refresh();
    }

    private static void insert(int ck, String duration) {
        String query = "INSERT INTO %s (pk, ck, duration, text) VALUES (0, %d, %s, '%s');";
        utils.execute(String.format(query, utils.getQualifiedTable(), ck, duration, duration));
    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    @Test
    public void testAll() {
        utils.searchAll().checkOrderedColumns("ck", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testMatchDuration() {
        testMatch("duration");
    }

    @Test
    public void testMatchText() {
        testMatch("text");
    }

    private void testMatch(String column) {
        utils.filter(match(column, "1ns")).checkOrderedColumns("ck", 2)
             .filter(match(column, "1mo")).checkOrderedColumns("ck", 1)
             .filter(match(column, "1y2mo3w4d5h6m7s")).checkOrderedColumns("ck", 0)
             .filter(match(column, "1y2mo3w4d5h6m7s").docValues(true)).checkOrderedColumns("ck", 0)
             .filter(match(column, "1m")).checkOrderedColumns("ck", 5, 6)
             .filter(match(column, "-2m")).checkOrderedColumns("ck", 9)
             .filter(match(column, "1h1m")).checkOrderedColumns("ck", 8);
    }

    @Test
    public void testRangeDuration() {
        testRange("duration");
    }

    @Test
    public void testRangeText() {
        testRange("text");
    }

    private void testRange(String column) {
        utils.filter(range(column).lower("1y2mo3w4d5h6m7s")).check(0)
             .filter(range(column).lower("1y2mo3w4d5h6m7s").includeLower(true)).checkOrderedColumns("ck", 0)
             .filter(range(column).upper("-1m")).checkOrderedColumns("ck", 9)
             .filter(range(column).upper("-1m").docValues(true)).checkOrderedColumns("ck", 9)
             .filter(range(column).lower("59s").upper("61s")).checkOrderedColumns("ck", 5, 6)
             .filter(range(column).lower("59s").upper("61s").includeLower(true)).checkOrderedColumns("ck", 4, 5, 6)
             .filter(range(column).lower("59s").upper("61s").includeUpper(true)).checkOrderedColumns("ck", 5, 6, 7);
    }

    @Test
    public void testSortDuration() {
        testSort("duration");
    }

    @Test
    public void testSortText() {
        testSort("text");
    }

    private void testSort(String column) {
        utils.sort(field(column).reverse(false)).checkOrderedColumns("ck", 9, 2, 3, 4, 5, 6, 7, 8, 1, 0)
             .sort(field(column).reverse(true)).checkOrderedColumns("ck", 0, 1, 8, 7, 5, 6, 4, 3, 2, 9);
    }

    @Test
    public void testMalformedInsertion() {
        try {
            utils.insert("pk,ck,text", 1, 1, "0");
            Assert.fail("Expected InvalidQueryException");
        } catch (InvalidQueryException e) {
            Assert.assertEquals("Field 'text' requires a duration, but found '0'", e.getMessage());
        }
    }
}
