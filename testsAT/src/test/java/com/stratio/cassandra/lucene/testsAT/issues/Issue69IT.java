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

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Issue69IT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("distinct")
                              .withPartitionKey("make")
                              .withClusteringKey("model")
                              .withColumn("make", "text")
                              .withColumn("model", "text")
                              .withColumn("color", "text")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(new String[]{"make", "model", "color"}, new Object[]{"Tesla", "Model X", "Red"})
                              .insert(new String[]{"make", "model", "color"}, new Object[]{"Tesla", "Model S", "Red"})
                              .insert(new String[]{"make", "model", "color"}, new Object[]{"Porsche", "Cayman S", "Red"})
                              .refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    @Test
    public void testUDF() {
        int n1 = utils.execute("SELECT make FROM %s;", utils.getQualifiedTable()).all().size();
        assertEquals("Basic count is wrong", n1, 3);
        int n2 = utils.execute("SELECT DISTINCT make FROM %s;", utils.getQualifiedTable()).all().size();
        assertEquals("Basic count is wrong", n2, 2);
    }
}
