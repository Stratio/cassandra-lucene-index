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

import com.stratio.cassandra.lucene.key.KeyMapper;
import org.apache.cassandra.db.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.util.Optional;

/**
 * A entry of the {@link SearchCache}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchCacheEntry {

    private final SearchCache searchCache;
    private final String search;
    private final ReadCommand command;
    private final PartitionPosition currentPosition;
    private final Optional<Clustering> currentClustering;
    private final ScoreDoc scoreDoc;
    private final Query query;
    private final PartitionPosition startPosition;
    private final PartitionPosition stopPosition;
    private final Optional<ClusteringPrefix> startPrefix;
    private final Optional<ClusteringPrefix> stopPrefix;

    SearchCacheEntry(SearchCache searchCache, String search, PartitionRangeReadCommand command, Query query) {
        this.searchCache = searchCache;
        this.search = search;
        this.command = command;
        this.query = query;
        startPosition = command.dataRange().startKey();
        stopPosition = command.dataRange().stopKey();
        startPrefix = KeyMapper.startClusteringPrefix(command.dataRange());
        stopPrefix = KeyMapper.stopClusteringPrefix(command.dataRange());
        currentPosition = startPosition;
        currentClustering = startPrefix.isPresent()
                            ? Optional.of(new Clustering(startPrefix.get().getRawValues()))
                            : Optional.empty();
        scoreDoc = null;
    }

    SearchCacheEntry(SearchCache searchCache,
                     String search,
                     PartitionRangeReadCommand command,
                     DecoratedKey decoratedKey,
                     Optional<Clustering> currentClustering,
                     ScoreDoc scoreDoc,
                     Query query) {
        this.searchCache = searchCache;
        this.search = search;
        this.command = command;
        this.currentPosition = decoratedKey;
        this.currentClustering = currentClustering;
        this.scoreDoc = scoreDoc;
        this.query = query;
        startPosition = command.dataRange().startKey();
        stopPosition = command.dataRange().stopKey();
        startPrefix = KeyMapper.startClusteringPrefix(command.dataRange());
        stopPrefix = KeyMapper.stopClusteringPrefix(command.dataRange());
    }

    boolean isValid(ClusteringComparator comparator, String search, PartitionRangeReadCommand command) {
        if (search.equals(this.search)) {
            DataRange dataRange = command.dataRange();
            return validKey(dataRange) && validPrefix(comparator, dataRange);
        }
        return false;
    }

    private boolean validKey(DataRange dataRange) {
        PartitionPosition start = dataRange.startKey();
        if (currentPosition.compareTo(start) == 0 && startPosition.compareTo(start) <= 0) {
            PartitionPosition stop = dataRange.stopKey();
            return stopPosition.compareTo(stop) == 0;
        }
        return false;
    }

    private boolean validPrefix(ClusteringComparator comparator, DataRange dataRange) {

        // Discard start prefix
        Optional<ClusteringPrefix> start = KeyMapper.startClusteringPrefix(dataRange);
        if (start.isPresent() && startPrefix.isPresent() && comparator.compare(startPrefix.get(), start.get()) > 0) {
            return false;
        }

        // Discard null clusterings
        if (start.isPresent() != currentClustering.isPresent()) {
            return false;
        }

        // Discard clustering
        if (start.isPresent() &&
            comparator.compare(new Clustering(start.get().getRawValues()), currentClustering.get()) != 0) {
            return false;
        }

        // Discard stop prefix
        Optional<ClusteringPrefix> stop = KeyMapper.stopClusteringPrefix(dataRange);
        if (stop.isPresent() && stopPrefix.isPresent() && comparator.compare(stopPrefix.get(), stop.get()) != 0) {
            return false;
        }

        return true;
    }

    /**
     * Returns a new {@link SearchCacheUpdater} for updating this entry.
     *
     * @return the cache updater
     */
    public SearchCacheUpdater updater() {
        return searchCache.updater(search, command, query);
    }

    /**
     * Returns the cached {@link ScoreDoc}.
     *
     * @return the score of the last cached position
     */
    public ScoreDoc getScoreDoc() {
        return scoreDoc;
    }

    /**
     * Returns the cached {@link Query}.
     *
     * @return the cached Lucene's query
     */
    public Query getQuery() {
        return query;
    }
}
