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
package com.stratio.cassandra.lucene.testsAT.bitemporal;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.bitemporal;
import static com.stratio.cassandra.lucene.builder.Builder.bitemporalMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class FutureBitemporalAT extends BaseAT {

    protected static CassandraUtils cassandraUtils;

    public static final Map<String, String> data1;
    public static final Map<String, String> data2;
    public static final Map<String, String> data3;

    static {

        data1 = new LinkedHashMap<>();
        data1.put("integer_1", "1");
        data1.put("vt_from", "'2015/01/01 00:00:00.000'");
        data1.put("vt_to", "'2200/01/01 00:00:00.000'");
        data1.put("tt_from", "'2015/01/01 12:00:00.001'");
        data1.put("tt_to", "'2015/01/05 12:00:00.000'");

        data2 = new LinkedHashMap<>();
        data2.put("integer_1", "2");
        data2.put("vt_from", "'2015/01/01 12:00:00.001'");
        data2.put("vt_to", "'2015/01/05 12:00:00.000'");
        data2.put("tt_from", "'2015/01/05 12:00:00.001'");
        data2.put("tt_to", "'2015/01/10 12:00:00.000'");

        data3 = new LinkedHashMap<>();
        data3.put("integer_1", "3");
        data3.put("vt_from", "'2015/01/05 12:00:00.001'");
        data3.put("vt_to", "'2200/01/01 00:00:00.000'");
        data3.put("tt_from", "'2015/01/10 12:00:00.001'");
        data3.put("tt_to", "'2200/01/01 00:00:00.000'");

    }

    @BeforeClass
    public static void setUpSuite() throws InterruptedException {
        cassandraUtils = CassandraUtils.builder("bitemporal_future")
                                       .withPartitionKey("integer_1")
                                       .withClusteringKey()
                                       .withColumn("integer_1", "int")
                                       .withColumn("vt_from", "text")
                                       .withColumn("vt_to", "text")
                                       .withColumn("tt_from", "text")
                                       .withColumn("tt_to", "text")
                                       .withColumn("lucene", "text")
                                       .withMapper("bitemporal",
                                                   bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(
                                                           "yyyy/MM/dd HH:mm:ss.SSS").nowValue(
                                                           "2200/01/01 00:00:00.000"))
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1, data2, data3);
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropKeyspace();
    }

    private String fromInteger(Integer[] list) {

        String out = "{";
        for (Integer aList : list) {
            out += Integer.toString(aList) + ",";
        }
        return out.substring(0, out.length() - 1) + "}";

    }

    private boolean isThisAndOnlyThis(Integer[] intList1, int[] intList2) {
        if (intList1.length != intList2.length) {
            return false;
        } else {

            for (Integer i : intList1) {
                boolean found = false;
                for (Integer j : intList2) {
                    if (i.equals(j)) {
                        found = true;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
    }

    @Test
    public void testFuture() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").ttFrom(
                "2015/01/02 12:00:00.001").ttTo("2015/01/02 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void testFuture2() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").ttFrom(
                "2015/01/06 12:00:00.001").ttTo("2015/01/06 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void testFuture3() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").ttFrom(
                "2015/01/15 12:00:00.001").ttTo("2015/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void testFuture4() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").vtFrom(
                "2016/01/15 12:00:00.001").vtTo("2016/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 3}));
    }

    @Test
    public void testFuture5() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").vtFrom("2015/06/15 12:00:00.001")
                                                                                    .vtTo("2015/07/15 12:00:00.001")
                                                                                    .ttFrom("2015/01/02 12:00:00.001")
                                                                                    .ttTo("2015/01/02 12:00:00.001"))
                                                    .refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void testFuture6() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporal("bitemporal").ttFrom(
                "2200/01/01 00:00:00.000").ttTo("2200/01/01 00:00:00.000")).refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

}
