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
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsUpdate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.bitemporal;
import static com.stratio.cassandra.lucene.builder.Builder.bitemporalMapper;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class BitemporalSearchIT extends BaseIT {

    private static final String SIMPLE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss.SSSZ";

    protected static CassandraUtils utils, utils2;

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
        data1.put("vt_from", "'2015/01/01 00:00:00.000+0000'");
        data1.put("vt_to", "'2015/02/01 12:00:00.000+0000'");
        data1.put("tt_from", "'2015/01/15 12:00:00.001+0000'");
        data1.put("tt_to", "'2015/02/15 12:00:00.000+0000'");

        data2 = new LinkedHashMap<>();
        data2.put("integer_1", "2");
        data2.put("vt_from", "'2015/02/01 12:00:00.001+0000'");
        data2.put("vt_to", "'2015/03/01 12:00:00.000+0000'");
        data2.put("tt_from", "'2015/02/15 12:00:00.001+0000'");
        data2.put("tt_to", "'2015/03/15 12:00:00.000+0000'");

        data3 = new LinkedHashMap<>();
        data3.put("integer_1", "3");
        data3.put("vt_from", "'2015/03/01 12:00:00.001+0000'");
        data3.put("vt_to", "'2015/04/01 12:00:00.000+0000'");
        data3.put("tt_from", "'2015/03/15 12:00:00.001+0000'");
        data3.put("tt_to", "'2015/04/15 12:00:00.000+0000'");

        data4 = new LinkedHashMap<>();
        data4.put("integer_1", "4");
        data4.put("vt_from", "'2015/04/01 12:00:00.001+0000'");
        data4.put("vt_to", "'2015/05/01 12:00:00.000+0000'");
        data4.put("tt_from", "'2015/04/15 12:00:00.001+0000'");
        data4.put("tt_to", "'2015/05/15 12:00:00.000+0000'");

        data5 = new LinkedHashMap<>();
        data5.put("integer_1", "5");
        data5.put("vt_from", "'2015/05/01 12:00:00.001+0000'");
        data5.put("vt_to", "'2015/06/01 12:00:00.000+0000'");
        data5.put("tt_from", "'2015/05/15 12:00:00.001+0000'");
        data5.put("tt_to", "'2015/06/15 12:00:00.000+0000'");

        data6 = new LinkedHashMap<>();
        data6.put("integer_1", "5");
        data6.put("vt_from", "'2016/05/01 12:00:00.001+0000'");
        data6.put("vt_to", "'2016/06/01 12:00:00.000+0000'");
        data6.put("tt_from", "'2016/05/15 12:00:00.001+0000'");
        data6.put("tt_to", "'2016/06/15 12:00:00.000+0000'");

        data7 = new LinkedHashMap<>();
        data7.put("id", "1");
        data7.put("data", "'v1'");
        data7.put("vt_from", "'1970/01/01 00:00:00.001+0000'");
        data7.put("vt_to", "'3000/01/01 00:00:00.000+0000'");
        data7.put("tt_from", "'1970/01/01 00:00:00.001+0000'");
        data7.put("tt_to", "'3000/01/01 00:00:00.000+0000'");

        data8 = new LinkedHashMap<>();
        data8.put("id", "2");
        data8.put("data", "'v1'");
        data8.put("vt_from", "'1970/01/01 00:00:00.001+0000'");
        data8.put("vt_to", "'3000/01/01 00:00:00.000+0000'");
        data8.put("tt_from", "'1970/01/01 00:00:00.001+0000'");
        data8.put("tt_to", "'3000/01/01 00:00:00.000+0000'");

        data9 = new LinkedHashMap<>();
        data9.put("id", "3");
        data9.put("data", "'v1'");
        data9.put("vt_from", "'1970/01/01 00:00:00.001+0000'");
        data9.put("vt_to", "'3000/01/01 00:00:00.000+0000'");
        data9.put("tt_from", "'1970/01/01 00:00:00.001+0000'");
        data9.put("tt_to", "'3000/01/01 00:00:00.000+0000'");

        data10 = new LinkedHashMap<>();
        data10.put("id", "4");
        data10.put("data", "'v1'");
        data10.put("vt_from", "'1970/01/01 00:00:00.001+0000'");
        data10.put("vt_to", "'3000/01/01 00:00:00.000+0000'");
        data10.put("tt_from", "'1970/01/01 00:00:00.001+0000'");
        data10.put("tt_to", "'3000/01/01 00:00:00.000+0000'");

        data11 = new LinkedHashMap<>();
        data11.put("id", "5");
        data11.put("data", "'v1'");
        data11.put("vt_from", "'1970/01/01 00:00:00.001+0000'");
        data11.put("vt_to", "'3000/01/01 00:00:00.000+0000'");
        data11.put("tt_from", "'1970/01/01 00:00:00.001+0000'");
        data11.put("tt_to", "'3000/01/01 00:00:00.000+0000'");

        data12 = new LinkedHashMap<>();
        data12.put("integer_1", "1");
        data12.put("vt_from", "'2015/01/01 00:00:00.000+0000'");
        data12.put("vt_to", "'2200/01/01 00:00:00.000+0000'");
        data12.put("tt_from", "'2015/01/01 12:00:00.001+0000'");
        data12.put("tt_to", "'2015/01/05 12:00:00.000+0000'");

        data13 = new LinkedHashMap<>();
        data13.put("integer_1", "2");
        data13.put("vt_from", "'2015/01/01 12:00:00.001+0000'");
        data13.put("vt_to", "'2015/01/05 12:00:00.000+0000'");
        data13.put("tt_from", "'2015/01/05 12:00:00.001+0000'");
        data13.put("tt_to", "'2015/01/10 12:00:00.000+0000'");

        data14 = new LinkedHashMap<>();
        data14.put("integer_1", "3");
        data14.put("vt_from", "'2015/01/05 12:00:00.001+0000'");
        data14.put("vt_to", "'2200/01/01 00:00:00.000+0000'");
        data14.put("tt_from", "'2015/01/10 12:00:00.001+0000'");
        data14.put("tt_to", "'2200/01/01 00:00:00.000+0000'");
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                                                .validated(true);
        utils = builder("bitemporal")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal", mapper)
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data1, data2, data3, data4, data5)
                .refresh();

        String nowValue = "2016/03/02 00:00:00.000+0000";
        utils2 = builder("bitemporal2")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to")
                                    .pattern(SIMPLE_DATE_PATTERN)
                                    .nowValue(nowValue)
                                    .validated(true))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
    }

    @AfterClass
    public static void afterClass() {
        utils.dropIndex().dropTable().dropKeyspace();
        utils2.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testBitemporalSearchIntersectsTimeStampField() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/01/01 00:00:00.000+0000")
                                            .vtTo("2015/02/01 12:00:00.000+0000")
                                            .ttFrom("2015/01/15 12:00:00.001+0000")
                                            .ttTo("2015/02/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 1);
    }

    @Test
    public void testBitemporalSearchIntersectsTimeStampField2() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/02/01 12:00:00.001+0000")
                                            .vtTo("2015/03/01 12:00:00.000+0000")
                                            .ttFrom("2015/02/15 12:00:00.001+0000")
                                            .ttTo("2015/03/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 2);
    }

    @Test
    public void testBitemporalQueryIntersectsTimeStampField3() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/03/01 12:00:00.001+0000")
                                            .vtTo("2015/04/01 12:00:00.000+0000")
                                            .ttFrom("2015/03/15 12:00:00.001+0000")
                                            .ttTo("2015/04/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 3);
    }

    @Test
    public void testBitemporalQueryIntersectsTimeStampField4() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/04/01 12:00:00.001+0000")
                                            .vtTo("2015/05/01 12:00:00.000+0000")
                                            .ttFrom("2015/04/15 12:00:00.001+0000")
                                            .ttTo("2015/05/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 4);
    }

    @Test
    public void testBitemporalQueryIntersectsTimeStampField5() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/05/01 12:00:00.001+0000")
                                            .vtTo("2015/06/01 12:00:00.000+0000")
                                            .ttFrom("2015/05/15 12:00:00.001+0000")
                                            .ttTo("2015/06/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 5);
    }

    @Test
    public void testBitemporalQueryIntersectsTimeStampField6() {
        utils.query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000+0000")
                                            .vtTo("2015/03/02 00:00:00.000+0000")
                                            .ttFrom("2015/01/14 00:00:00.000+0000")
                                            .ttTo("2015/04/02 00:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 1, 2, 3);
    }

    @Test
    public void testBitemporalQueryIntersectsTimeStampField7() {
        utils.query(bitemporal("bitemporal").vtFrom("2014/12/01 12:00:00.000+0000")
                                            .vtTo("2014/12/31 00:00:00.000+0000")
                                            .ttFrom("2015/01/14 00:00:00.000+0000")
                                            .ttTo("2015/04/02 00:00:00.000+0000"))
             .check(0);
    }

    @Test
    public void testBitemporalSearchIntersectsTimeStampField8() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/01/01 00:00:00.000+0000")
                                            .vtTo("2015/02/01 12:00:00.001+0000")
                                            .ttFrom("2015/01/15 12:00:00.001+0000")
                                            .ttTo("2015/02/15 12:00:00.001+0000"))
             .checkUnorderedColumns("integer_1", 1, 2);
    }

    @Test
    public void testBitemporalSearchIntersectsTimeStampField9() {
        utils.query(bitemporal("bitemporal").vtFrom("2015/02/01 12:00:00.000+0000")
                                            .vtTo("2015/03/01 12:00:00.000+0000")
                                            .ttFrom("2015/02/15 12:00:00.000+0000")
                                            .ttTo("2015/03/15 12:00:00.000+0000"))
             .checkUnorderedColumns("integer_1", 1, 2);
    }

    //inserting bigger to nowValue it
    @SuppressWarnings("unchecked")
    @Test
    public void testBitemporalQueryWithNowValueTooLong() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        utils2.insert(InvalidQueryException.class,
                      "BitemporalDateTime value '1462104000001' exceeds Max Value: '1456876800000'",
                      data6);
    }

    //vt_to>vt_from
    @SuppressWarnings("unchecked")
    @Test
    public void testBitemporalSearchWithTTToSmallerThanTTFrom() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        Map<String, String> data = new LinkedHashMap<>();
        data.put("integer_1", "5");
        data.put("vt_from", "'2016/01/01 00:01:00.001+0000'");
        data.put("vt_to", "'2016/01/02 00:01:00.001+0000'");
        data.put("tt_from", "'2016/02/01 00:01:00.001+0000'");
        data.put("tt_to", "'2016/01/01 00:01:00.001+0000'");

        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to")
                .pattern(SIMPLE_DATE_PATTERN)
                .validated(true);
        builder("bitemporal")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal", mapper)
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(InvalidQueryException.class, "is after", data)
                .dropKeyspace();
    }

    //tt_to<tt_from
    @SuppressWarnings("unchecked")
    @Test
    public void testBitemporalSearchWithVtToSmallerThanVTFrom() {
        // testing with long value 1456876800 == 2016/03/02 00:00:00
        Map<String, String> data = new LinkedHashMap<>();
        data.put("integer_1", "5");
        data.put("tt_from", "'2016/01/01 00:01:00.001+0000'");
        data.put("tt_to", "'2016/01/02 00:01:00.001+0000'");
        data.put("vt_from", "'2016/02/01 00:01:00.001+0000'");
        data.put("vt_to", "'2016/01/01 00:01:00.001+0000'");

        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to")
                .pattern(SIMPLE_DATE_PATTERN)
                .validated(true);
        builder("bitemporal")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal", mapper)
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(InvalidQueryException.class, "is after", data)
                .dropKeyspace();
    }

    //valid String max value queries setting nowValue to max date in data3
    @Test
    public void testBitemporalSearchIsWithInNowValueToString4() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000+0000";

        builder("bitemporal2")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to")
                                    .pattern(SIMPLE_DATE_PATTERN)
                                    .nowValue(nowValue)
                                    .validated(true))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data1, data2, data3)
                .refresh()
                .query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000+0000")
                                               .vtTo("2015/03/02 00:00:00.000+0000")
                                               .ttFrom("2015/01/14 00:00:00.000+0000")
                                               .ttTo("2015/04/02 00:00:00.000+0000"))
                .checkUnorderedColumns("integer_1", 1, 2, 3)
                .dropIndex().dropTable().dropKeyspace();
    }

    //querying without limits to vt
    @Test
    public void testBitemporalSearchIsWithInNowValueToString5() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000+0000";
        builder("bitemporal2")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to")
                                    .pattern(SIMPLE_DATE_PATTERN)
                                    .nowValue(nowValue)
                                    .validated(true))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data1, data2, data3)
                .refresh()
                .query(bitemporal("bitemporal").ttFrom(
                        "2015/01/14 00:00:00.000+0000")
                                               .ttTo("2015/04/02 00:00:00.000+0000"))
                .checkUnorderedColumns("integer_1", 1, 2, 3)
                .dropIndex().dropTable().dropKeyspace();
    }

    //querying without limits to tt
    @Test
    public void testBitemporalSearchIsWithInNowValueToString6() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000+0000";
        builder("bitemporal2")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to")
                                    .pattern(SIMPLE_DATE_PATTERN)
                                    .nowValue(nowValue)
                                    .validated(true))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data1, data2, data3)
                .refresh()
                .query(bitemporal("bitemporal").vtFrom("2014/12/31 12:00:00.000+0000")
                                               .vtTo("2015/03/02 00:00:00.000+0000"))
                .checkUnorderedColumns("integer_1", 1, 2, 3)
                .dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testBitemporalSearchOverBigIntsWithDefaultPattern() {
        String nowValue = "3000/01/01 00:00:00.000+0000";
        Mapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to")
                .pattern(SIMPLE_DATE_PATTERN).nowValue(nowValue);
        Batch batch = QueryBuilder.batch();
        CassandraUtils utils = builder("bitemporal3")
                .withPartitionKey("id")
                .withClusteringKey("vt_from", "tt_from")
                .withColumn("id", "int")
                .withColumn("data", "text")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal", mapper)
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data7, data8, data9, data10, data11)
                .refresh()
                .searchAll()
                .checkUnorderedColumns("id", 1, 2, 3, 4, 5);

        CassandraUtilsUpdate utilsUpdate = utils.update()
                                                .where("id", 1)
                                                .and("vt_from", "1970/01/01 00:00:00.001+0000")
                                                .and("tt_from", "1970/01/01 00:00:00.001+0000");

        utilsUpdate.onlyIf(QueryBuilder.eq("tt_to", nowValue))
                   .with(QueryBuilder.set("tt_to", "1970/01/01 05:35:50.101+0000"));

        batch.add(utilsUpdate.asUpdate());
        batch.add(utils.asInsert(new String[]{"id",
                                              "data",
                                              "vt_from",
                                              "vt_to",
                                              "tt_from",
                                              "tt_to"},
                                 new Object[]{1,
                                              "v2",
                                              "1970/01/01 00:00:00.001+0000",
                                              nowValue,
                                              "1970/01/01 05:35:50.102+0000",
                                              nowValue}));

        assertTrue("batch execution didn't work",
                   utils.execute(batch).wasApplied());

        utils.refresh();

        utils.filter(bitemporal("bitemporal").vtFrom("1970/01/01 00:00:00.001+0000")
                                             .vtTo(nowValue)
                                             .ttFrom(nowValue)
                                             .ttTo(nowValue))
             .checkUnorderedColumns("id", 1, 2, 3, 4, 5);

        CassandraUtilsSelect
                select
                = utils.filter(bitemporal("bitemporal").vtFrom("1970/01/01 00:00:00.001+0000")
                                                       .vtTo(nowValue)
                                                       .ttFrom(nowValue)
                                                       .ttTo(nowValue))
                       .and("AND id = 1");
        select.check(1);
        select.checkUnorderedColumns("id", 1);
        select.checkUnorderedColumns("data", "v2");
        select.checkUnorderedColumns("vt_from", "1970/01/01 00:00:00.001+0000");
        select.checkUnorderedColumns("vt_to", nowValue);
        select.checkUnorderedColumns("tt_from", "1970/01/01 05:35:50.102+0000");
        select.checkUnorderedColumns("tt_to", nowValue);
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture1() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern(
                                    "yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue(
                                                             "2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").ttFrom(
                        "2015/01/02 12:00:00.001+0000")
                                                .ttTo("2015/01/02 12:00:00.001+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 1)
                .dropIndex()
                .dropTable()
                .dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture2() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue("2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").ttFrom("2015/01/06 12:00:00.001+0000")
                                                .ttTo("2015/01/06 12:00:00.001+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 2)
                .dropIndex()
                .dropTable()
                .dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture3() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue("2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").ttFrom("2015/01/15 12:00:00.001+0000")
                                                .ttTo("2015/01/15 12:00:00.001+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 3)
                .dropIndex()
                .dropTable()
                .dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture4() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue("2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").vtFrom("2016/01/15 12:00:00.001+0000")
                                                .vtTo("2016/01/15 12:00:00.001+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 1, 3)
                .dropIndex()
                .dropTable()
                .dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture5() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue("2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").vtFrom(
                        "2015/06/15 12:00:00.001+0000")
                                                .vtTo("2015/07/15 12:00:00.001+0000")
                                                .ttFrom("2015/01/02 12:00:00.001+0000")
                                                .ttTo("2015/01/02 12:00:00.001+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 1)
                .dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testBitemporalSearchFuture6() {
        builder("bitemporal_future")
                .withPartitionKey("integer_1")
                .withClusteringKey()
                .withColumn("integer_1", "int")
                .withColumn("vt_from", "text")
                .withColumn("vt_to", "text")
                .withColumn("tt_from", "text")
                .withColumn("tt_to", "text")
                .withMapper("bitemporal",
                            bitemporalMapper("vt_from",
                                             "vt_to",
                                             "tt_from",
                                             "tt_to").pattern("yyyy/MM/dd HH:mm:ss.SSS")
                                                     .nowValue("2200/01/01 00:00:00.000+0000"))
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data12, data13, data14)
                .refresh()
                .filter(bitemporal("bitemporal").ttFrom("2200/01/01 00:00:00.000+0000")
                                                .ttTo("2200/01/01 00:00:00.000+0000"))
                .refresh(true)
                .checkUnorderedColumns("integer_1", 3)
                .dropIndex()
                .dropTable()
                .dropKeyspace();
    }
}
