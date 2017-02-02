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
import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test partitioning on virtual nodes partition. This test does not depends on virtual tokens in cassandra because
 * vnodes partitioning also works with 1 token range (num_tokens==1, initial_token != null)
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class PartitioningOnVirtualNodesIT {

    private static Integer numTokens;
    private static CassandraUtilsBuilder cassandraUtilsBuilder;
    private static final String numTokensAttribute = "org.apache.cassandra.db:type=StorageService";

    @BeforeClass
    public static void beforeClass() {
        CassandraConnection.connect();
        List<List<String>> tokens = CassandraConnection.getJMXAttribute(numTokensAttribute, "Tokens");
        numTokens = tokens.get(0).size();
        cassandraUtilsBuilder = builder("virtual_nodes_partitioning")
                .withTable("test")
                .withIndexName("idx")
                .withColumn("pk1", "int")
                .withColumn("pk2", "int")
                .withColumn("ck", "int")
                .withColumn("rc", "int")
                .withIndexColumn(null)
                .withPartitionKey("pk1", "pk2")
                .withClusteringKey("ck");

        cassandraUtilsBuilder.build()
                             .createKeyspace()
                             .createTable()
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 0, 0, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 0, 1, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 1, 0, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{0, 1, 1, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 0, 0, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 0, 1, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 1, 0, 0})
                             .insert(new String[]{"pk1", "pk2", "ck", "rc"}, new Object[]{1, 1, 1, 0});
    }

    @AfterClass
    public static void after() {
        cassandraUtilsBuilder.build().dropKeyspace();
    }

    private void testWithVirtualNodesPerPartition(String indexName, Integer vNodesPerPartition) {
        if (vNodesPerPartition == 0) {
            vNodesPerPartition = 1;
        }
        cassandraUtilsBuilder.withPartitioner(new Partitioner.OnVirtualNode(vNodesPerPartition))
                             .withIndexName(indexName)
                             .build()
                             .createIndex()
                             .refresh()
                             .filter(all()).fetchSize(2).check(8)
                             .filter(all()).andEq("pk1", 0).andEq("pk2", 0).fetchSize(2).check(2)
                             .filter(all()).andEq("pk1", 0).andEq("pk2", 0).andEq("ck", 0).fetchSize(2).check(1)
                             .filter(all()).andEq("pk1", 0).allowFiltering(true).fetchSize(2).check(4)
                             .filter(all()).andEq("pk1", 1).allowFiltering(true).fetchSize(2).check(4)
                             .dropIndex();
    }

    @Test
    public void testVNodesWithOneVNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_one_vnode_per_partition", 1);
    }

    @Test
    public void testVNodesWithNumTokensVNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_num_tokens_vnode_per_partition", numTokens);
    }

    @Test
    public void testVNodesWithOneAndAHalfNumTokensVNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_one_and_a_half_num_tokens_vnode_per_partition",
                                         Math.round(numTokens * 1.5f));
    }

    @Test
    public void testVNodesWithFloorNumTokensDiv2VNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_floor_num_tokens_div_2_vnode_per_partition",
                                         Math.floorDiv(Math.round(numTokens), 2));
    }

    @Test
    public void testVNodesWithCeilNumTokensDiv2VNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_ceil_num_tokens_div_2_vnode_per_partition",
                                         (int) Math.ceil(numTokens / 2.0f));
    }

    @Test
    public void testVNodesWithFloorNumTokensDiv3VNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_floor_num_tokens_div_3_vnode_per_partition",
                                         Math.floorDiv(Math.round(numTokens), 3));
    }

    @Test
    public void testVNodesWithCeilNumTokensDiv3VNodePerPartition() {
        testWithVirtualNodesPerPartition("idx_test_vnodes_with_ceil_num_tokens_div_3_vnode_per_partition",
                                         (int) Math.ceil(numTokens / 3.0f));
    }
}
