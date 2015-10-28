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

import junit.framework.Assert;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Test;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SearchResultTest {

    @Test
    public void testConstructor() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        Assert.assertNotNull("SearchResult constructor must not return a null object", searchResult);

    }

    @Test
    public void testGetPartitionKey() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        Assert.assertEquals("SearchResult getPartitionKey must return the same object passed as parameter",
                            decoratedKey,
                            searchResult.getPartitionKey());

    }

    @Test
    public void testGetClusteringKey() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        Assert.assertEquals("SearchResult getClusteringKey must return the same object passed as parameter",
                            clusteringKey,
                            searchResult.getClusteringKey());

    }

    @Test
    public void testGetScoreDoc() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        Assert.assertEquals("SearchResult getScoreDoc must not return a null object",
                            scoreDoc,
                            searchResult.getScoreDoc());
    }

    @Test
    public void testEquals() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);
        SearchResult searchResult2 = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        assertTrue("SearchResult constructed with same parameters must return equals",
                   searchResult.equals(searchResult2));

    }

    @Test
    public void testEqualsSameObject() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult1 = new SearchResult(decoratedKey, clusteringKey, scoreDoc);
        SearchResult searchResult2 = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        assertTrue("SearchResult equals(SearchResult) must return true with the same object",
                   searchResult1.equals(searchResult2));
    }

    @Test
    public void testEqualsNull() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);
        assertFalse("SearchResult equals(SearchResult) must return false with null object", searchResult.equals(null));
    }

    @Test
    public void testEqualsObject() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);
        Object dummy = new Object();
        assertFalse("SearchResult equals(SearchResult) must return true with another object",
                    searchResult.equals(dummy));
    }

    @Test
    public void testEqualsSameScoreDoc() {
        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);

        DecoratedKey decoratedKey2 = new BufferDecoratedKey(new LongToken((long) 35), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey2 = CellNames.simpleSparse(new ColumnIdentifier("bbbb", true));
        ScoreDoc scoreDoc2 = new ScoreDoc(1, 7.0f);
        SearchResult searchResult2 = new SearchResult(decoratedKey2, clusteringKey2, scoreDoc2);

        assertTrue("SearchResult object with same scoreDoc.doc, equals(SearchResult) must return true ",
                   searchResult.equals(searchResult2));
    }

    @Test
    public void testHashDoc() {

        DecoratedKey decoratedKey = new BufferDecoratedKey(new LongToken((long) 10), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        CellName clusteringKey = CellNames.simpleSparse(new ColumnIdentifier("aaaa", false));
        ScoreDoc scoreDoc = new ScoreDoc(1, 2.0f);
        SearchResult searchResult = new SearchResult(decoratedKey, clusteringKey, scoreDoc);
        assertEquals("SearchResult hashCode must be equals to scoreDoc.doc", scoreDoc.doc, searchResult.hashCode());

    }

}
