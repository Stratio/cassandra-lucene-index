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

/**
 * A entry of the {@link SearchCache}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchCacheEntry {

    private final SearchCache searchCache;
    private final String search;
    private final ReadCommand command;
    private final PartitionPosition position;
    private final Clustering clustering;
    private final ScoreDoc scoreDoc;
    private final Query query;
    private final PartitionPosition startPosition;
    private final PartitionPosition stopPosition;
    private final ClusteringPrefix startPrefix;
    private final ClusteringPrefix stopPrefix;

    public SearchCacheEntry(SearchCache searchCache,
                            String search,
                            PartitionRangeReadCommand command,
                            Query query) {
        this.searchCache = searchCache;
        this.search = search;
        this.command = command;
        this.query = query;
        startPosition = command.dataRange().startKey();
        stopPosition = command.dataRange().stopKey();
        startPrefix = KeyMapper.startClusteringPrefix(command.dataRange());
        stopPrefix = KeyMapper.stopClusteringPrefix(command.dataRange());
        position = startPosition;
        clustering = startPrefix == null ? null : new Clustering(startPrefix.getRawValues());
        scoreDoc = null;
    }

    public SearchCacheEntry(SearchCache searchCache,
                            String search,
                            PartitionRangeReadCommand command,
                            DecoratedKey decoratedKey,
                            Clustering clustering,
                            ScoreDoc scoreDoc,
                            Query query) {
        this.searchCache = searchCache;
        this.search = search;
        this.command = command;
        this.position = decoratedKey;
        this.clustering = clustering;
        this.scoreDoc = scoreDoc;
        this.query = query;
        startPosition = command.dataRange().startKey();
        stopPosition = command.dataRange().stopKey();
        startPrefix = KeyMapper.startClusteringPrefix(command.dataRange());
        stopPrefix = KeyMapper.stopClusteringPrefix(command.dataRange());
    }

    public boolean isValid(ClusteringComparator comparator, String search, PartitionRangeReadCommand command) {

        if (!search.equals(this.search)) {
            return false;
        }

        DataRange dataRange = command.dataRange();
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();

        // Discard key
        if (position.compareTo(startPosition) != 0) {
            return false;
        }

        // Discard start position
        if (this.startPosition.compareTo(startPosition) > 0) {
            return false;
        }

        // Discard stop position
        if (this.stopPosition.compareTo(stopPosition) != 0) {
            return false;
        }

        // Discard start prefix
        ClusteringPrefix startPrefix = KeyMapper.startClusteringPrefix(dataRange);
        if (startPrefix != null && this.startPrefix != null && comparator.compare(this.startPrefix, startPrefix) > 0) {
            return false;
        }

        // Discard null clusterings
        if (startPrefix == null && clustering != null || startPrefix != null && clustering == null) {
            return false;
        }

        // Discard clustering
        if (startPrefix != null && comparator.compare(new Clustering(startPrefix.getRawValues()), clustering) != 0) {
            return false;
        }

        // Discard stop prefix
        ClusteringPrefix stopPrefix = KeyMapper.stopClusteringPrefix(dataRange);
        if (stopPrefix != null && this.stopPrefix != null && comparator.compare(this.stopPrefix, stopPrefix) != 0) {
            return false;
        }

        return true;
    }

    public SearchCacheUpdater updater() {
        return searchCache.updater(search, command, query);
    }

    public ScoreDoc getScoreDoc() {
        return scoreDoc;
    }

    public Query getQuery() {
        return query;
    }
}
