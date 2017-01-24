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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test partitioning on virtual nodes partition. This test does not depends on virtual tokens in cassandra because
 * vnodes partitioning also works with 1 token range (num_tokens==1, initial_token != null)
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class PartitioningOnVirtualNodesIT extends BaseIT {

    private List<Integer> vNodesPerPartition;
    private static final String TokensAtrreibute= "org.apache.cassandra.db:type=StorageService";


    private void addVNodesPerPartition(Integer vNodePerPartition) {
        if (vNodePerPartition>0) {
            vNodesPerPartition.add(vNodePerPartition);
        }
    }
    @Before
    public void before() {
        vNodesPerPartition = new ArrayList<>();
        List<Object> tokens = CassandraConnection.getJMXAttribute(TokensAtrreibute, "Tokens");

        Float numTokens = (float) tokens.size();

        addVNodesPerPartition(1);
        addVNodesPerPartition(Math.round(numTokens));
        addVNodesPerPartition(Math.round(numTokens * 1.5f));

        addVNodesPerPartition(Math.floorDiv(Math.round(numTokens), 2));
        addVNodesPerPartition((int) Math.ceil(numTokens / 2.0f));
        addVNodesPerPartition(Math.floorDiv(Math.round(numTokens), 3));
        addVNodesPerPartition((int) Math.ceil(numTokens / 3.0f));
    }

    public void testWithVirtualNodesPerPartition(Integer vnodesPerPartition) {
        builder("virtual_nodes_partitioning")
                .withTable("test")
                .withIndexName("idx")
                .withColumn("pk1", "int")
                .withColumn("pk2", "int")
                .withColumn("ck", "int")
                .withColumn("rc", "int")
                .withIndexColumn("lucene")
                .withPartitionKey("pk1", "pk2")
                .withClusteringKey("ck")
                .withPartitioner(new Partitioner.OnVirtualNode(vnodesPerPartition))
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
                .dropKeyspace();
    }

    @Test
    public void testVNodesPartitioning() {
        vNodesPerPartition.forEach(this::testWithVirtualNodesPerPartition);
    }
}
