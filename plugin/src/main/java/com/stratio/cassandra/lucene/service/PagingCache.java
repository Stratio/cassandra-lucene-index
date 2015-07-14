/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.stratio.cassandra.lucene.search.Search;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DataRange.Paging;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.Composite;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.lang.reflect.Field;

/**
 * Cache remembering Lucene {@link Query} positions associated to Cassandra {@link Row}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PagingCache {

    private static final Character KEY_SEPARATOR = '@';

    private final Cache<String, Entry> cache;

    public PagingCache(int size) {
        cache = CacheBuilder.newBuilder().maximumSize(size).concurrencyLevel(1).build();
    }

    public Entry get(Search search, DataRange dataRange) {
        if (dataRange instanceof Paging) {
            try {
                String key = key(search, (Paging) dataRange);
                return cache.getIfPresent(key);
            } catch (ReflectiveOperationException e) {
                return null;
            }
        }
        return null;
    }

    public void put(Search search, DataRange dataRange, Query query, ScoredRow scoredRow) {
        if (dataRange instanceof Paging) {
            Row row = scoredRow.getRow();
            String key = key(search, (Paging) dataRange, row);
            Entry entry = new Entry(scoredRow.getScoreDoc(), query);
            cache.put(key, entry);
        }
    }

    public void clear() {
        cache.invalidateAll();
    }

    private String key(Search search, Paging paging, Row row) {
        RowPosition startKey = row.key;
        RowPosition stopKey = paging.stopKey();
        Composite startName = row.cf.getReverseSortedColumns().iterator().next().name();
        return key(search, startKey, stopKey, startName);
    }

    private String key(Search search, Paging paging) throws ReflectiveOperationException {
        RowPosition startKey = paging.startKey();
        RowPosition stopKey = paging.stopKey();
        Composite startName = startName(paging);
        return key(search, startKey, stopKey, startName);
    }

    private String key(Search search, RowPosition startKey, RowPosition stopKey, Composite startName) {
        String hash = search.toString();
        hash += KEY_SEPARATOR;
        hash += startKey.hashCode();
        hash += KEY_SEPARATOR;
        hash += stopKey.hashCode();
        hash += KEY_SEPARATOR;
        hash += startName.hashCode();
        return hash;
    }

    private Composite startName(Paging paging) throws ReflectiveOperationException {
        Field field = paging.getClass().getDeclaredField("firstPartitionColumnStart");
        field.setAccessible(true);
        return (Composite) field.get(paging);
    }

    public static class Entry {

        private ScoreDoc scoreDoc;
        private Query query;

        public Entry(ScoreDoc scoreDoc, Query query) {
            this.scoreDoc = scoreDoc;
            this.query = query;
        }

        public ScoreDoc getScoreDoc() {
            return scoreDoc;
        }

        public Query getQuery() {
            return query;
        }
    }
}
