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

package com.stratio.cassandra.lucene.testsAT.search;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
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
public class BitemporalSearchAT extends BaseAT {

    private static final String TIMESTAMP_PATTERN = "timestamp";
    private static final String SIMPLE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

    protected static CassandraUtils cassandraUtils;

    public static final Map<String, String> data1;
    public static final Map<String, String> data2;
    public static final Map<String, String> data3;
    public static final Map<String, String> data4;
    public static final Map<String, String> data5;
    public static final Map<String, String> data6;
    public static final Map<String, String> data7;
    public static final Map<String, String> data8;
    public static final Map<String, String> data9;
    public static final Map<String, String> data10;
    public static final Map<String, String> data11;
    public static final Map<String, String> data12;
    public static final Map<String, String> data13;
    public static final Map<String, String> data14;

    static {

        data1 = new LinkedHashMap<>();
        data1.put("integer_1", "1");
        data1.put("vt_from", "'2015/01/01 00:00:00.000'");
        data1.put("vt_to", "'2015/02/01 12:00:00.000'");
        data1.put("tt_from", "'2015/01/15 12:00:00.001'");
        data1.put("tt_to", "'2015/02/15 12:00:00.000'");

        data2 = new LinkedHashMap<>();
        data2.put("integer_1", "2");
        data2.put("vt_from", "'2015/02/01 12:00:00.001'");
        data2.put("vt_to", "'2015/03/01 12:00:00.000'");
        data2.put("tt_from", "'2015/02/15 12:00:00.001'");
        data2.put("tt_to", "'2015/03/15 12:00:00.000'");

        data3 = new LinkedHashMap<>();
        data3.put("integer_1", "3");
        data3.put("vt_from", "'2015/03/01 12:00:00.001'");
        data3.put("vt_to", "'2015/04/01 12:00:00.000'");
        data3.put("tt_from", "'2015/03/15 12:00:00.001'");
        data3.put("tt_to", "'2015/04/15 12:00:00.000'");

        data4 = new LinkedHashMap<>();
        data4.put("integer_1", "4");
        data4.put("vt_from", "'2015/04/01 12:00:00.001'");
        data4.put("vt_to", "'2015/05/01 12:00:00.000'");
        data4.put("tt_from", "'2015/04/15 12:00:00.001'");
        data4.put("tt_to", "'2015/05/15 12:00:00.000'");

        data5 = new LinkedHashMap<>();
        data5.put("integer_1", "5");
        data5.put("vt_from", "'2015/05/01 12:00:00.001'");
        data5.put("vt_to", "'2015/06/01 12:00:00.000'");
        data5.put("tt_from", "'2015/05/15 12:00:00.001'");
        data5.put("tt_to", "'2015/06/15 12:00:00.000'");

        data6 = new LinkedHashMap<>();
        data6.put("integer_1", "5");
        data6.put("vt_from", "'2016/05/01 12:00:00.001'");
        data6.put("vt_to", "'2016/06/01 12:00:00.000'");
        data6.put("tt_from", "'2016/05/15 12:00:00.001'");
        data6.put("tt_to", "'2016/06/15 12:00:00.000'");

        data7 = new LinkedHashMap<>();
        data7.put("id", "1");
        data7.put("data", "'v1'");
        data7.put("vt_from", "0");
        data7.put("vt_to", "9223372036854775807");
        data7.put("tt_from", "0");
        data7.put("tt_to", "9223372036854775807");

        data8 = new LinkedHashMap<>();
        data8.put("id", "2");
        data8.put("data", "'v1'");
        data8.put("vt_from", "0");
        data8.put("vt_to", "9223372036854775807");
        data8.put("tt_from", "0");
        data8.put("tt_to", "9223372036854775807");

        data9 = new LinkedHashMap<>();
        data9.put("id", "3");
        data9.put("data", "'v1'");
        data9.put("vt_from", "0");
        data9.put("vt_to", "9223372036854775807");
        data9.put("tt_from", "0");
        data9.put("tt_to", "9223372036854775807");

        data10 = new LinkedHashMap<>();
        data10.put("id", "4");
        data10.put("data", "'v1'");
        data10.put("vt_from", "0");
        data10.put("vt_to", "9223372036854775807");
        data10.put("tt_from", "0");
        data10.put("tt_to", "9223372036854775807");

        data11 = new LinkedHashMap<>();
        data11.put("id", "5");
        data11.put("data", "'v1'");
        data11.put("vt_from", "0");
        data11.put("vt_to", "9223372036854775807");
        data11.put("tt_from", "0");
        data11.put("tt_to", "9223372036854775807");

        data12 = new LinkedHashMap<>();
        data12.put("integer_1", "1");
        data12.put("vt_from", "'2015/01/01 00:00:00.000'");
        data12.put("vt_to", "'2200/01/01 00:00:00.000'");
        data12.put("tt_from", "'2015/01/01 12:00:00.001'");
        data12.put("tt_to", "'2015/01/05 12:00:00.000'");

        data13 = new LinkedHashMap<>();
        data13.put("integer_1", "2");
        data13.put("vt_from", "'2015/01/01 12:00:00.001'");
        data13.put("vt_to", "'2015/01/05 12:00:00.000'");
        data13.put("tt_from", "'2015/01/05 12:00:00.001'");
        data13.put("tt_to", "'2015/01/10 12:00:00.000'");

        data14 = new LinkedHashMap<>();
        data14.put("integer_1", "3");
        data14.put("vt_from", "'2015/01/05 12:00:00.001'");
        data14.put("vt_to", "'2200/01/01 00:00:00.000'");
        data14.put("tt_from", "'2015/01/10 12:00:00.001'");
        data14.put("tt_to", "'2200/01/01 00:00:00.000'");

    }

    @BeforeClass
    public static void setUpSuite() throws InterruptedException {
        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS");
        cassandraUtils = CassandraUtils.builder("bitemporal")
                                       .withPartitionKey("integer_1")
                                       .withClusteringKey()
                                       .withColumn("integer_1", "int")
                                       .withColumn("vt_from", "text")
                                       .withColumn("vt_to", "text")
                                       .withColumn("tt_from", "text")
                                       .withColumn("tt_to", "text")
                                       .withColumn("lucene", "text")
                                       .withMapper("bitemporal", mapper)
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1, data2, data3, data4, data5);
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
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
    public void biTemporalQueryIntersectsTimeStampFieldTest() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/01/01 00:00:00.000")
                                                                                   .vtTo("2015/02/01 12:00:00.000")
                                                                                   .ttFrom("2015/01/15 12:00:00.001")
                                                                                   .ttTo("2015/02/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest2() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/02/01 12:00:00.001")
                                                                                   .vtTo("2015/03/01 12:00:00.000")
                                                                                   .ttFrom("2015/02/15 12:00:00.001")
                                                                                   .ttTo("2015/03/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest3() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/03/01 12:00:00.001")
                                                                                   .vtTo("2015/04/01 12:00:00.000")
                                                                                   .ttFrom("2015/03/15 12:00:00.001")
                                                                                   .ttTo("2015/04/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest4() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/04/01 12:00:00.001")
                                                                                   .vtTo("2015/05/01 12:00:00.000")
                                                                                   .ttFrom("2015/04/15 12:00:00.001")
                                                                                   .ttTo("2015/05/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest5() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/05/01 12:00:00.001")
                                                                                   .vtTo("2015/06/01 12:00:00.000")
                                                                                   .ttFrom("2015/05/15 12:00:00.001")
                                                                                   .ttTo("2015/06/15 12:00:00.000"));
        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest6() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000")
                                                                                   .vtTo("2015/03/02 00:00:00.000")
                                                                                   .ttFrom("2015/01/14 00:00:00.000")
                                                                                   .ttTo("2015/04/02 00:00:00.000"));
        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest7() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2014/12/01 12:00:00.000")
                                                                                   .vtTo("2014/12/31 00:00:00.000")
                                                                                   .ttFrom("2015/01/14 00:00:00.000")
                                                                                   .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest8() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/01/01 00:00:00.000")
                                                                                   .vtTo("2015/02/01 12:00:00.001")
                                                                                   .ttFrom("2015/01/15 12:00:00.001")
                                                                                   .ttTo("2015/02/15 12:00:00.001"));
        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    @Test
    public void biTemporalQueryIntersectsTimeStampFieldTest9() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporal("bitemporal").vtFrom("2015/02/01 12:00:00.000")
                                                                                   .vtTo("2015/03/01 12:00:00.000")
                                                                                   .ttFrom("2015/02/15 12:00:00.000")
                                                                                   .ttTo("2015/03/15 12:00:00.000"));
        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    private CassandraUtils setUpSuite2(Object nowValue, String pattern) {
        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(pattern)
                                                                                .nowValue(nowValue)
                                                                                .validated(true);
        return CassandraUtils.builder("bitemporal2")
                             .withPartitionKey("integer_1")
                             .withClusteringKey()
                             .withColumn("integer_1", "int")
                             .withColumn("vt_from", "text")
                             .withColumn("vt_to", "text")
                             .withColumn("tt_from", "text")
                             .withColumn("tt_to", "text")
                             .withColumn("lucene", "text")
                             .withMapper("bitemporal", mapper)
                             .build()
                             .createKeyspace()
                             .createTable()
                             .createIndex();
    }

    private CassandraUtils setUpSuite4() {
        return CassandraUtils.builder("bitemporal_future")
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
                                                 "yyyy/MM/dd HH:mm:ss.SSS").nowValue("2200/01/01 00:00:00.000"))
                             .build()
                             .createKeyspace()
                             .createTable()
                             .createIndex()
                             .insert(data12, data13, data14);

    }

    private void tearDown(CassandraUtils cu) {
        cu.dropIndex().dropTable().dropKeyspace();
    }

    //inserting bigger to nowValue it
    @Test(expected = InvalidQueryException.class)
    public void biTemporalQueryWithNowValueTooLongTest() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = setUpSuite2(nowValue, SIMPLE_DATE_PATTERN);
        try {
            cu.insert(data6);
        } finally {
            tearDown(cu);
        }
    }

    //vt_to>vt_from
    @Test(expected = InvalidQueryException.class)
    public void biTemporalQueryWithttToSmallerThanTTFrom() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        Map<String, String> data = new LinkedHashMap<>();
        data.put("id", "5");
        data.put("data", "'v1'");
        data.put("vt_from", "0");
        data.put("vt_to", "9223372036854775807");
        data.put("tt_from", "9223372036854775807");
        data.put("tt_to", "0");
        cassandraUtils.insert(data);
    }

    //tt_to<tt_from
    @Test(expected = InvalidQueryException.class)
    public void biTemporalQueryWithVtToSmallerhanVTFrom() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        Map<String, String> data = new LinkedHashMap<>();
        data.put("id", "5");
        data.put("data", "'v1'");
        data.put("vt_from", "9223372036854775807");
        data.put("vt_to", "0");
        data.put("tt_from", "0");
        data.put("tt_to", "9223372036854775807");
        cassandraUtils.insert(data);
    }

    //valid String max value queries setting nowValue to max date in data3
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest4() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = setUpSuite2(nowValue, SIMPLE_DATE_PATTERN).insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max
        CassandraUtilsSelect select = cu.query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000")
                                                                       .vtTo("2015/03/02 00:00:00.000")
                                                                       .ttFrom("2015/01/14 00:00:00.000")
                                                                       .ttTo("2015/04/02 00:00:00.000"));
        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
        tearDown(cu);
    }

    //querying without limits to vt
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest5() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = setUpSuite2(nowValue, SIMPLE_DATE_PATTERN).insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max
        CassandraUtilsSelect select = cu.query(bitemporal("bitemporal").ttFrom("2015/01/14 00:00:00.000")
                                                                       .ttTo("2015/04/02 00:00:00.000"));
        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
        tearDown(cu);
    }

    //querying without limits to tt
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest6() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = setUpSuite2(nowValue, SIMPLE_DATE_PATTERN).insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max
        CassandraUtilsSelect select = cu.query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000")
                                                                       .vtTo("2015/03/02 00:00:00.000"));
        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
        tearDown(cu);
    }

    @Test
    public void biTemporalQueryOverBigIntsWithDefaultPattern() {

        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(TIMESTAMP_PATTERN);
        CassandraUtils cu = CassandraUtils.builder("bitemporal3")
                                          .withPartitionKey("id")
                                          .withClusteringKey("vt_from", "tt_from")
                                          .withColumn("id", "int")
                                          .withColumn("data", "text")
                                          .withColumn("vt_from", "bigint")
                                          .withColumn("vt_to", "bigint")
                                          .withColumn("tt_from", "bigint")
                                          .withColumn("tt_to", "bigint")
                                          .withColumn("lucene", "text")
                                          .withMapper("bitemporal", mapper)
                                          .build()
                                          .createKeyspace()
                                          .createTable()
                                          .createIndex()
                                          .insert(data7, data8, data9, data10, data11);

        CassandraUtilsSelect select = cu.searchAll();

        assertEquals("Expected 5 results!", 5, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select.intColumn("id")),
                   isThisAndOnlyThis(select.intColumn("id"), new int[]{1, 2, 3, 4, 5}));

        Batch batch = QueryBuilder.batch();
        Update update = QueryBuilder.update(cu.getKeyspace(), cu.getTable());

        update.where(QueryBuilder.eq("id", 1))
              .and(QueryBuilder.eq("vt_from", 0))
              .and(QueryBuilder.eq("tt_from", 0))
              .onlyIf(QueryBuilder.eq("tt_to", 9223372036854775807L))
              .with(QueryBuilder.set("tt_to", 20150101));

        batch.add(update);

        Insert insert = QueryBuilder.insertInto(cu.getKeyspace(), cu.getTable())
                                    .value("id", 1)
                                    .value("data", "v2")
                                    .value("vt_from", 0)
                                    .value("vt_to", 9223372036854775807L)
                                    .value("tt_from", 20150102)
                                    .value("tt_to", 9223372036854775807L);

        batch.add(insert);
        ResultSet result = cu.execute(batch);

        assertTrue("batch execution didn't work", result.wasApplied());

        CassandraUtilsSelect select2 = cu.filter(bitemporal("bitemporal").vtFrom(0)
                                                                         .vtTo(9223372036854775807L)
                                                                         .ttFrom(9223372036854775807L)
                                                                         .ttTo(9223372036854775807L)).refresh(true);

        assertEquals("Expected 5 results!", 5, select2.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select2.intColumn("id")),
                   isThisAndOnlyThis(select2.intColumn("id"), new int[]{1, 2, 3, 4, 5}));

        CassandraUtilsSelect select3 = cu.filter(bitemporal("bitemporal").vtFrom(0)
                                                                         .vtTo(9223372036854775807L)
                                                                         .ttFrom(9223372036854775807L)
                                                                         .ttTo(9223372036854775807L)).and("AND id = 1");

        assertEquals("Expected 1 results!", 1, select3.count());
        Row row = select3.get().get(0);

        assertTrue("Unexpected results!! Expected result : {id=\"1\"}, got: " + row.getInt("id"),
                   row.getInt("id") == 1);
        assertTrue("Unexpected results!! Expected result : {data=\"v2\"}, got: " + row.getString("data"),
                   row.getString("data").equals("v2"));
        assertTrue("Unexpected results!! Expected result : {vt_from=0}, got: " + row.getLong("vt_from"),
                   row.getLong("vt_from") == 0L);
        assertTrue("Unexpected results!! Expected result : {vt_to=0}, got: " + row.getLong("vt_to"),
                   row.getLong("vt_to") == 9223372036854775807L);
        assertTrue("Unexpected results!! Expected result : {tt_from=0}, got: " + row.getLong("tt_from"),
                   row.getLong("tt_from") == 20150102L);
        assertTrue("Unexpected results!! Expected result : {tt_to=0}, got: " + row.getLong("tt_to"),
                   row.getLong("tt_to") == 9223372036854775807L);
        tearDown(cu);

    }

    @Test
    public void testFuture() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").ttFrom("2015/01/02 12:00:00.001")
                                                                        .ttTo("2015/01/02 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
        tearDown(cu);
    }

    @Test
    public void testFuture2() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").ttFrom("2015/01/06 12:00:00.001")
                                                                        .ttTo("2015/01/06 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
        tearDown(cu);
    }

    @Test
    public void testFuture3() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").ttFrom("2015/01/15 12:00:00.001")
                                                                        .ttTo("2015/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
        tearDown(cu);
    }

    @Test
    public void testFuture4() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").vtFrom("2016/01/15 12:00:00.001")
                                                                        .vtTo("2016/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 3}));
        tearDown(cu);
    }

    @Test
    public void testFuture5() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").vtFrom("2015/06/15 12:00:00.001")
                                                                        .vtTo("2015/07/15 12:00:00.001")
                                                                        .ttFrom("2015/01/02 12:00:00.001")
                                                                        .ttTo("2015/01/02 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
        tearDown(cu);
    }

    @Test
    public void testFuture6() {
        CassandraUtils cu = setUpSuite4();
        CassandraUtilsSelect select = cu.filter(bitemporal("bitemporal").ttFrom("2200/01/01 00:00:00.000")
                                                                        .ttTo("2200/01/01 00:00:00.000")).refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
        tearDown(cu);
    }
}
