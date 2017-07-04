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
package com.stratio.cassandra.lucene.testsAT.varia;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.field;

/**
 * Test the retrieval of large volumes of rows, specially above 65535 rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class SearchMatchingManyRowsAndSkipIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("search_matching_many_rows_and_skip")
                              .withPartitionKey("pk")
                              .withClusteringKey("ck")
                              .withColumn("pk", "int")
                              .withColumn("ck", "int")
                              .withColumn("rc", "int")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();

        int numPartitions = 100;
        int partitionSize = 100;
        String[] names = new String[]{"pk", "ck", "rc"};
        for (Integer pk = 0; pk < numPartitions; pk++) {
            List<Object[]> values = new ArrayList<>();
            for (Integer ck = 0; ck < partitionSize; ck++) {
                values.add(new Object[]{pk, ck, pk * partitionSize + ck});
            }
            utils.insert(names, values);
        }
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testSkipQuery() {
        utils.query(all()).fetchSize(65536).limit(65536).skip(5000).check(5000);
    }

    @Test
    public void testFailingSkipQuery() {
        utils.query(all()).fetchSize(10000).limit(65536).skip(5000).check(InvalidQueryException.class, "Search 'skip' option is not compatible with paging.");
    }

    @Test
    public void testSkipFilter() {
        utils.filter(all()).fetchSize(65536).limit(65536).skip(1001).check(8999);
    }

    @Test
    public void testFailingSkipFilter() {
        utils.filter(all()).fetchSize(10000).limit(65536).skip(1001).check(InvalidQueryException.class, "Search 'skip' option is not compatible with paging.");
    }

    @Test
    public void testSkipSort() {
        utils.sort(field("rc")).fetchSize(65536).limit(65536).skip(2501).check(7499);
    }

    @Test
    public void testFailingSkipSort() {
        utils.sort(field("rc")).fetchSize(10000).limit(65536).skip(2501).check(InvalidQueryException.class, "Search 'skip' option is not compatible with paging.");
    }
}
