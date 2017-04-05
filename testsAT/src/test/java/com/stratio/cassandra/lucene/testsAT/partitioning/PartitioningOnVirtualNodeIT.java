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
package com.stratio.cassandra.lucene.testsAT.partitioning;

import com.stratio.cassandra.lucene.builder.index.Partitioner;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test index partitioning on virtual node. This test class ignores some tests if cassandra target
 * cluster is not configured with virtual tokens (numTokens in cassandra.yaml).
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class PartitioningOnVirtualNodeIT extends BaseIT {

    private static Integer numTokens = 1;
    private static final String storageServiceMBean = "org.apache.cassandra.db:type=StorageService";

    @BeforeClass
    public static void beforeClass() {
        List<List<String>> tokens = CassandraConnection.getJMXAttribute(storageServiceMBean, "Tokens");
        numTokens = tokens.get(0).size();
    }

    public static void test(int vNodesPerPartition) {
        if ((numTokens == 1) && ((vNodesPerPartition > 1) || (vNodesPerPartition < 1))) {
            logger.debug("[IGNORED] vnode test because target cassandra cluster is not configured with virtual tokens.");
        } else {
            builder("virtual_nodes_partitioning")
                    .withTable("test")
                    .withIndexName("idx")
                    .withColumn("pk1", "int")
                    .withColumn("pk2", "int")
                    .withColumn("ck", "int")
                    .withColumn("rc", "int")
                    .withIndexColumn(null)
                    .withPartitionKey("pk1", "pk2")
                    .withClusteringKey("ck")
                    .withPartitioner(new Partitioner.OnVirtualNode(vNodesPerPartition))
                    .build()
                    .createKeyspace()
                    .createTable()
                    .createIndex()
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 0, 0, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 0, 1, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 1, 0, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 1, 1, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 0, 0, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 0, 1, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 1, 0, 0})
                    .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 1, 1, 0})
                    .refresh()
                    .filter(all()).fetchSize(2).check(8)
                    .filter(all()).andEq("pk1", 0).andEq("pk2", 0).fetchSize(2).check(2)
                    .filter(all()).andEq("pk1", 0).andEq("pk2", 0).andEq("ck", 0).fetchSize(2).check(1)
                    .filter(all()).andEq("pk1", 0).allowFiltering(true).fetchSize(2).check(4)
                    .filter(all()).andEq("pk1", 1).allowFiltering(true).fetchSize(2).check(4)
                    .dropIndex()
                    .dropKeyspace();
        }
    }

    @Test
    public void testVNodesWithOneVNodePerPartition() {
        test(1);
    }

    @Test
    public void testVNodesWithNumTokensVNodePerPartition() {
        test(numTokens);
    }

    @Test
    public void testVNodesWithOneAndAHalfNumTokensVNodePerPartition() {
        test(Math.round(numTokens * 1.5f));
    }

    @Test
    public void testVNodesWithFloorNumTokensDiv2VNodePerPartition() {
        test(Math.floorDiv(Math.round(numTokens), 2));
    }

    @Test
    public void testVNodesWithFloorNumTokensDiv3VNodePerPartition() {
        test(Math.floorDiv(Math.round(numTokens), 3));
    }
}
