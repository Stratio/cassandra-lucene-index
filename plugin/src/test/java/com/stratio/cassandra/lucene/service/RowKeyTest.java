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

package com.stratio.cassandra.lucene.service;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import static org.junit.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class RowKeyTest {

    @Test
    public void tesRowKey() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        assertEquals("RowKey.getPartitionKey must return the same passsed as parameter",
                     rowKey.getPartitionKey(),
                     decoratedKey);

        assertEquals("RowKey.getClusteringKey must return the same passsed as parameter",
                     rowKey.getClusteringKey(),
                     clusteringKey);
    }

    @Test
    public void testToString() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        assertEquals("RowKey.toString must return",
                     "RowKey{partitionKey=DecoratedKey(10, ), " + "clusteringKey=" + clusteringKey.toString() + "}",
                     rowKey.toString());
    }

}
