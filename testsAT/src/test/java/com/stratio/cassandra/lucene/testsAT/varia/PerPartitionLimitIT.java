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
import com.datastax.driver.core.exceptions.SyntaxError;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

/**
 * Tests PER PARTITION LIMIT clause.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class PerPartitionLimitIT extends BaseIT {

    @Test
    public void testWide() {
        CassandraUtils utils = CassandraUtils.builder("per_partition_limit")
                                             .withPartitionKey("pk")
                                             .withClusteringKey("ck")
                                             .withColumn("pk", "int")
                                             .withColumn("ck", "int")
                                             .withColumn("rc", "int")
                                             .build()
                                             .createKeyspace()
                                             .createTable();
        for (int pk = 0; pk < 2; pk++) {
            for (int ck = 0; ck < 5; ck++) {
                utils.insert("pk,ck,rc", pk, ck, pk * ck + ck);
            }
        }
        utils.createIndex().refresh();
        try {
            utils.execute("SELECT * FROM %s WHERE expr(%s,'{}') PER PARTITION LIMIT 10",
                          utils.getQualifiedTable(),
                          utils.getIndexName()).all();
        } catch (InvalidQueryException e) {
            assertEquals("PER PARTITION LIMIT exception message is wrong",
                         "Lucene index doesn't support PER PARTITION LIMIT",
                         e.getMessage());
        } catch (SyntaxError e) {
            logger.info("Skipping PER PARTITION LIMIT test because this release doesn't support it");
        } finally {
            CassandraUtils.dropKeyspaceIfNotNull(utils);
        }
    }

    @Test
    public void testSkinny() {
        CassandraUtils utils = CassandraUtils.builder("per_partition_limit")
                                             .withPartitionKey("pk")
                                             .withColumn("pk", "int")
                                             .withColumn("rc", "int")
                                             .build()
                                             .createKeyspace()
                                             .createTable();
        for (int pk = 0; pk < 10; pk++) {
            utils.insert("pk,rc", pk, pk);
        }
        utils.createIndex().refresh();
        try {
            utils.execute("SELECT * FROM %s WHERE expr(%s,'{}') PER PARTITION LIMIT 10",
                          utils.getQualifiedTable(),
                          utils.getIndexName()).all();
        } catch (InvalidQueryException e) {
            assertEquals("PER PARTITION LIMIT exception message is wrong",
                         "Lucene index doesn't support PER PARTITION LIMIT",
                         e.getMessage());
        } catch (SyntaxError e) {
            logger.info("Skipping PER PARTITION LIMIT test because this release doesn't support it");
        } finally {
            CassandraUtils.dropKeyspaceIfNotNull(utils);
        }
    }

}
