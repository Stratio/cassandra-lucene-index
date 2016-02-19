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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.lucene.search.Query;

import java.util.Optional;
import java.util.UUID;

/**
 * Search cache to take advantage of Lucene's query cache.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchCache {

    private final ClusteringComparator comparator;
    private final Cache<UUID, SearchCacheEntry> cache;

    public SearchCache(CFMetaData metadata, int cacheSize) {
        this.comparator = metadata.comparator;
        this.cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    }

    void put(UUID key, SearchCacheEntry entry) {
        cache.put(key, entry);
    }

    public void put(String search, ReadCommand command, Query query) {
        if (command instanceof PartitionRangeReadCommand) {
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            put(UUID.randomUUID(), new SearchCacheEntry(this, search, rangeCommand, query));
        }
    }

    public SearchCacheUpdater updater(String search, ReadCommand command, Query query) {
        return new SearchCacheUpdater(this, search, UUID.randomUUID(), command, query);
    }

    public void invalidate() {
        cache.invalidateAll();
    }

    public Optional<SearchCacheEntry> get(String search, ReadCommand command) {
        if (command instanceof PartitionRangeReadCommand) {
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            return cache.asMap()
                        .values()
                        .stream()
                        .filter(e -> e.isValid(comparator, search, rangeCommand))
                        .findAny();
        }
        return Optional.empty();
    }

}
