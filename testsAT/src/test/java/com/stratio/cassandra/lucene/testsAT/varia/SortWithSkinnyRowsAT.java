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
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.field;
import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class SortWithSkinnyRowsAT extends BaseAT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("sort_with_wide_rows")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int", integerMapper())
                              .withColumn("rc", "int", integerMapper())
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (int i = 0; i < 10; i++) {
            utils.insert(new String[]{"pk", "rc"}, new Object[]{i, i});
        }
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void sortAsc() {
        utils.sort(field("rc").reverse(false)).limit(3).checkOrderedColumns("rc", 0, 1, 2);
    }

    @Test
    public void sortDesc() {
        utils.sort(field("rc").reverse(true)).limit(3).checkOrderedColumns("rc", 9, 8, 7);
    }

    @Test
    public void sortPartitionAsc() {
        utils.sort(field("rc").reverse(false)).andEq("pk", 1).limit(3).checkUnorderedColumns("rc", 1);
    }

    @Test
    public void sortPartitionDesc() {
        utils.sort(field("rc").reverse(true)).andEq("pk", 1).limit(3).checkUnorderedColumns("rc", 1);
    }

    @Test
    public void sortTokenRangeAsc() {
        utils.sort(field("rc").reverse(false))
             .andGt("token(pk)", 0)
             .limit(3)
             .checkOrderedColumns("rc", 3, 6, 7);
    }

    @Test
    public void sortTokenRangeDesc() {
        utils.sort(field("rc").reverse(true))
             .andGt("token(pk)", 0)
             .limit(3)
             .checkOrderedColumns("rc", 9, 7, 6);
    }

    @Test
    public void sortNotExistingColumn() {
        utils.sort(field("missing").reverse(true))
             .check(InvalidQueryException.class, "No mapper found for sortFields field 'missing'");
    }
}
