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

import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class RowKeysTest {

    @Test
    public void testConstructorRowKeys() {
        RowKeys rowKeys = new RowKeys();

        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        rowKeys.add(rowKey);

        assertEquals("rowsKeys.add 1, size must return 1", 1, rowKeys.size());

        DecoratedKey decoratedKey2 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey2 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey2 = new RowKey(decoratedKey2, clusteringKey2);

        rowKeys.add(rowKey2);

        assertEquals("rowsKeys.add 2, size() must return 2", 2, rowKeys.size());
    }

    @Test
    public void testListConstructor() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        DecoratedKey decoratedKey2 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey2 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey2 = new RowKey(decoratedKey2, clusteringKey2);

        List<RowKey> list = new ArrayList<>();

        list.add(rowKey);
        list.add(rowKey2);
        RowKeys rowKeys = new RowKeys(list);
        assertEquals("rowsKeys.add 2, size() must return 2", 2, rowKeys.size());

    }

    @Test
    public void testIterator() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        DecoratedKey decoratedKey2 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey2 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey2 = new RowKey(decoratedKey2, clusteringKey2);

        DecoratedKey decoratedKey3 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey3 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey3 = new RowKey(decoratedKey3, clusteringKey3);

        DecoratedKey decoratedKey4 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey4 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey4 = new RowKey(decoratedKey4, clusteringKey4);

        RowKeys rowKeys = new RowKeys();
        rowKeys.add(rowKey);
        rowKeys.add(rowKey2);
        rowKeys.add(rowKey3);
        rowKeys.add(rowKey4);

        int num = 4;
        for (RowKey rowKeyAux : rowKeys) {
            assertFalse("iterator returns a item that is npot added",
                        rowKeyAux != rowKey && rowKeyAux != rowKey2 && rowKeyAux != rowKey3 && rowKeyAux != rowKey4);
            num--;
        }
        assertEquals("iterator return more items than expected ", num, 0);

    }

    @Test
    public void testToString() {

        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey = new RowKey(decoratedKey, clusteringKey);

        DecoratedKey decoratedKey2 = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey2 = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        RowKey rowKey2 = new RowKey(decoratedKey2, clusteringKey2);

        List<RowKey> list = new ArrayList<>();

        list.add(rowKey);
        list.add(rowKey2);
        RowKeys rowKeys = new RowKeys(list);
        assertEquals("iterator return more items than expected ",
                     "RowKeys{rowKeys=[" + rowKey.toString() + ", " + rowKey2.toString() + "]}",
                     rowKeys.toString());
    }
}
