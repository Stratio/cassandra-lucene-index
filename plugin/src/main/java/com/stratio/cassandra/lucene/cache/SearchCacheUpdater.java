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

package com.stratio.cassandra.lucene.cache;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.util.Optional;
import java.util.UUID;

/**
 * A cache updater to update a cache entry.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchCacheUpdater {

    private final UUID id;
    private final SearchCache cache;
    private final String search;
    private final PartitionRangeReadCommand command;
    private final Query query;

    SearchCacheUpdater(SearchCache cache, String search, UUID id, ReadCommand command, Query query) {
        this.cache = cache;
        this.search = search;
        this.id = id;
        this.command = command instanceof PartitionRangeReadCommand ? (PartitionRangeReadCommand) command : null;
        this.query = query;
    }

    /**
     * Updates the cached entry with the specified pointer to a search result.
     *
     * @param key the row partition key
     * @param clustering the row clustering key
     * @param scoreDoc the row score for the query
     */
    public void put(DecoratedKey key, Clustering clustering, ScoreDoc scoreDoc) {
        if (command != null) {
            cache.put(id, new SearchCacheEntry(cache, search, command, key, Optional.of(clustering), scoreDoc, query));
        }
    }

    /**
     * Updates the cached entry with the specified pointer to a search result.
     *
     * @param key the row partition key
     * @param scoreDoc the row score for the query
     */
    public void put(DecoratedKey key, ScoreDoc scoreDoc) {
        if (command != null) {
            cache.put(id, new SearchCacheEntry(cache, search, command, key, Optional.empty(), scoreDoc, query));
        }
    }

}
