/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.service.RowKey;
import com.stratio.cassandra.lucene.service.RowKeys;
import com.stratio.cassandra.lucene.service.RowMapper;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.cassandra.service.StorageProxy.LocalRangeSliceRunnable;

@SuppressWarnings("unchecked")
public class LuceneStorageProxy {

    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private static final Method getLiveSortedEndpoints;
    private static final Method intersection;
    private static final Method calculateResultRowsUsingEstimatedKeys;

    static {
        try {
            Class<?> clazz = StorageProxy.class;

            getLiveSortedEndpoints = clazz.getDeclaredMethod("getLiveSortedEndpoints",
                                                             Keyspace.class,
                                                             RingPosition.class);
            getLiveSortedEndpoints.setAccessible(true);

            intersection = clazz.getDeclaredMethod("intersection", List.class, List.class);
            intersection.setAccessible(true);

            calculateResultRowsUsingEstimatedKeys = clazz.getDeclaredMethod("calculateResultRowsUsingEstimatedKeys",
                                                                            ColumnFamilyStore.class);
            calculateResultRowsUsingEstimatedKeys.setAccessible(true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition<?> pos) throws Exception {
        return (List<InetAddress>) getLiveSortedEndpoints.invoke(StorageProxy.instance, keyspace, pos);
    }

    @SuppressWarnings("unchecked")
    public static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2) throws Exception {
        return (List<InetAddress>) intersection.invoke(StorageProxy.instance, l1, l2);
    }

    private static float calculateResultRowsUsingEstimatedKeys(ColumnFamilyStore cfs) throws Exception {
        return (float) calculateResultRowsUsingEstimatedKeys.invoke(StorageProxy.instance, cfs);
    }

    public static float estimateResultRowsPerRange(Keyspace keyspace,
                                                   String columnFamily,
                                                   List<IndexExpression> rowFilter,
                                                   boolean countCQL3Rows) throws Exception {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamily);
        float resultRowsPerRange = Float.POSITIVE_INFINITY;
        if (rowFilter != null && !rowFilter.isEmpty()) {
            List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersForQuery(rowFilter);
            if (searchers.isEmpty()) {
                resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
            } else {
                // Secondary index query (cql3 or otherwise).  Estimate result rows based on most selective 2ary index.
                for (SecondaryIndexSearcher searcher : searchers) {
                    // use our own mean column count as our estimate for how many matching rows each node will have
                    SecondaryIndex highestSelectivityIndex = searcher.highestSelectivityIndex(rowFilter);
                    resultRowsPerRange = Math.min(resultRowsPerRange, highestSelectivityIndex.estimateResultRows());
                }
            }
        } else if (!countCQL3Rows) {
            // non-cql3 query
            resultRowsPerRange = cfs.estimateKeys();
        } else {
            resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
        }

        // adjust resultRowsPerRange by the number of tokens this node has and the replication factor for this ks
        return (resultRowsPerRange / DatabaseDescriptor.getNumTokens()) /
               keyspace.getReplicationStrategy().getReplicationFactor();
    }

    public static boolean ignoredTombstonedPartitions(IDiskAtomFilter predicate) {
        return predicate instanceof SliceQueryFilter &&
               ((SliceQueryFilter) predicate).compositesToGroup == SliceQueryFilter.IGNORE_TOMBSTONED_PARTITIONS;
    }

    private static RowKey rowKey(AbstractBounds<RowPosition> range, RowKeys rowKeys) {
        if (rowKeys == null) return null;
        for (RowKey rowKey : rowKeys) {
            DecoratedKey key = rowKey.getPartitionKey();
            if (range.contains(key)) return rowKey;
        }
        return null;
    }

    public static RowKey last(RowMapper mapper, RowKey rowKey, List<Row> rows, List<Row> processedRows) {
        for (int i = rows.size() - 1; i >= 0; i--) {
            Row row = rows.get(i);
            if (processedRows.contains(row)) return mapper.rowKey(row);
        }
        return rowKey;
    }

    public static Pair<List<Row>, RowKeys> getRangeSlice(IndexSearcher searcher,
                                                         String keyspaceName,
                                                         String columnFamily,
                                                         long timestamp,
                                                         IDiskAtomFilter predicate,
                                                         AbstractBounds<RowPosition> keyRange,
                                                         List<IndexExpression> expressions,
                                                         int limit,
                                                         ConsistencyLevel consistency_level,
                                                         RowKeys rowKeys,
                                                         boolean countCQL3Rows) throws Exception {
        Tracing.trace("Computing ranges to query");
        long startTime = System.nanoTime();

        Keyspace keyspace = Keyspace.open(keyspaceName);
        List<Row> rows;
        Map<AbstractBounds<RowPosition>, List<Row>> rowsPerRange = new HashMap<>();
        // now scan until we have enough results
        try {
            int liveRowCount = 0;
            boolean countLiveRows = countCQL3Rows || ignoredTombstonedPartitions(predicate);
            rows = new ArrayList<>();

            // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
            // expensive in clusters with vnodes)
            List<? extends AbstractBounds<RowPosition>> ranges;
            if (keyspace.getReplicationStrategy() instanceof LocalStrategy) ranges = keyRange.unwrap();
            else ranges = StorageProxy.getRestrictedRanges(keyRange);

            // determine the number of rows to be fetched and the concurrency factor
            int rowsToBeFetched = limit;
            int concurrencyFactor;
            if (searcher.requiresScanningAllRanges(expressions)) {
                // all nodes must be queried
                rowsToBeFetched *= ranges.size();
                concurrencyFactor = ranges.size();
                logger.debug("Requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                             limit,
                             ranges.size(),
                             concurrencyFactor);
                Tracing.trace("Submitting range requests on {} ranges with a concurrency of {}",
                              new Object[]{ranges.size(), concurrencyFactor});
            } else {
                // our estimate of how many result rows there will be per-range
                float resultRowsPerRange = estimateResultRowsPerRange(keyspace,
                                                                      columnFamily,
                                                                      expressions,
                                                                      countCQL3Rows);
                // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
                // fetch enough rows in the first round
                resultRowsPerRange -= resultRowsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
                concurrencyFactor = resultRowsPerRange == 0.0 ?
                                    1 :
                                    Math.max(1, Math.min(ranges.size(), (int) Math.ceil(limit / resultRowsPerRange)));
                logger.debug(
                        "Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                        resultRowsPerRange,
                        limit,
                        ranges.size(),
                        concurrencyFactor);
                Tracing.trace(
                        "Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)",
                        new Object[]{ranges.size(), concurrencyFactor, resultRowsPerRange});
            }

            boolean haveSufficientRows = false;
            int i = 0;
            AbstractBounds<RowPosition> nextRange = null;
            List<InetAddress> nextEndpoints = null;
            List<InetAddress> nextFilteredEndpoints = null;
            while (i < ranges.size()) {
                List<Pair<AbstractRangeCommand, ReadCallback<RangeSliceReply, Iterable<Row>>>> scanHandlers = new ArrayList<>(
                        concurrencyFactor);
                int concurrentFetchStartingIndex = i;
                int concurrentRequests = 0;
                while ((i - concurrentFetchStartingIndex) < concurrencyFactor) {
                    AbstractBounds<RowPosition> range = nextRange == null ? ranges.get(i) : nextRange;
                    List<InetAddress> liveEndpoints = nextEndpoints == null ?
                                                      getLiveSortedEndpoints(keyspace, range.right) :
                                                      nextEndpoints;
                    List<InetAddress> filteredEndpoints = nextFilteredEndpoints == null ?
                                                          consistency_level.filterForQuery(keyspace, liveEndpoints) :
                                                          nextFilteredEndpoints;
                    ++i;
                    ++concurrentRequests;

                    // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
                    // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
                    // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
                    while (i < ranges.size()) {
                        nextRange = ranges.get(i);
                        nextEndpoints = getLiveSortedEndpoints(keyspace, nextRange.right);
                        nextFilteredEndpoints = consistency_level.filterForQuery(keyspace, nextEndpoints);

                        // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                        // don't know how to deal with a wrapping range.
                        // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                        // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                        // wire compatibility, so It's likely easier not to bother;
                        if (range.right.isMinimum()) break;

                        List<InetAddress> merged = intersection(liveEndpoints, nextEndpoints);

                        // Check if there is enough endpoint for the merge to be possible.
                        if (!consistency_level.isSufficientLiveNodes(keyspace, merged)) break;

                        List<InetAddress> filteredMerged = consistency_level.filterForQuery(keyspace, merged);

                        // Estimate whether merging will be a win or not
                        if (!DatabaseDescriptor.getEndpointSnitch()
                                               .isWorthMergingForRangeQuery(filteredMerged,
                                                                            filteredEndpoints,
                                                                            nextFilteredEndpoints)) break;

                        // If we get there, merge this range and the next one
                        range = range.withNewRight(nextRange.right);
                        liveEndpoints = merged;
                        filteredEndpoints = filteredMerged;
                        ++i;
                    }

                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    RowKey after = rowKey(range, rowKeys);

                    List<IndexExpression> decoratedExpressions = new ArrayList<>(expressions);
                    if (after != null) {
                        decoratedExpressions.add(new IndexExpression(IndexSearcher.AFTER,
                                                                     Operator.EQ,
                                                                     searcher.mapper().byteBuffer(after)));
                    }
                    RangeSliceCommand command = new RangeSliceCommand(keyspaceName,
                                                                      columnFamily,
                                                                      timestamp,
                                                                      predicate,
                                                                      range,
                                                                      decoratedExpressions,
                                                                      limit);
                    rowsPerRange.put(range, new ArrayList<Row>());
                    ////////////////////////////////////////////////////////////////////////////////////////////////////

                    AbstractRangeCommand nodeCmd = command.forSubRange(range);

                    // collect replies and resolve according to consistency level
                    RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(nodeCmd.keyspace, timestamp);
                    List<InetAddress> minimalEndpoints = filteredEndpoints.subList(0,
                                                                                   Math.min(filteredEndpoints.size(),
                                                                                            consistency_level.blockFor(
                                                                                                    keyspace)));
                    ReadCallback<RangeSliceReply, Iterable<Row>> handler = new ReadCallback<>(resolver,
                                                                                              consistency_level,
                                                                                              nodeCmd,
                                                                                              minimalEndpoints);
                    handler.assureSufficientLiveNodes();
                    resolver.setSources(filteredEndpoints);
                    if (filteredEndpoints.size() == 1 &&
                        filteredEndpoints.get(0).equals(FBUtilities.getBroadcastAddress())) {
                        StageManager.getStage(Stage.READ)
                                    .execute(new LocalRangeSliceRunnable(nodeCmd, handler), Tracing.instance.get());
                    } else {
                        MessageOut<? extends AbstractRangeCommand> message = nodeCmd.createMessage();
                        for (InetAddress endpoint : filteredEndpoints) {
                            Tracing.trace("Enqueuing request to {}", endpoint);
                            MessagingService.instance().sendRR(message, endpoint, handler);
                        }
                    }
                    scanHandlers.add(Pair.create(nodeCmd, handler));
                }
                Tracing.trace("Submitted {} concurrent range requests covering {} ranges",
                              concurrentRequests,
                              i - concurrentFetchStartingIndex);

                List<AsyncOneResponse> repairResponses = new ArrayList<>();
                for (Pair<AbstractRangeCommand, ReadCallback<RangeSliceReply, Iterable<Row>>> cmdPairHandler : scanHandlers) {
                    ReadCallback<RangeSliceReply, Iterable<Row>> handler = cmdPairHandler.right;
                    RangeSliceResponseResolver resolver = (RangeSliceResponseResolver) handler.resolver;

                    try {
                        for (Row row : handler.get()) {
                            rows.add(row);
                            rowsPerRange.get(cmdPairHandler.left.keyRange).add(row);
                            if (countLiveRows) liveRowCount += row.getLiveCount(predicate, timestamp);
                        }
                        repairResponses.addAll(resolver.repairResults);
                    } catch (ReadTimeoutException ex) {
                        // we timed out waiting for responses
                        int blockFor = consistency_level.blockFor(keyspace);
                        int responseCount = resolver.responses.size();
                        String gotData = responseCount > 0 ?
                                         resolver.isDataPresent() ? " (including data)" : " (only digests)" :
                                         "";

                        if (Tracing.isTracing()) {
                            Tracing.trace("Timed out; received {} of {} responses{} for range {} of {}",
                                          new Object[]{responseCount, blockFor, gotData, i, ranges.size()});
                        } else if (logger.isDebugEnabled()) {
                            logger.debug("Range slice timeout; received {} of {} responses{} for range {} of {}",
                                         responseCount,
                                         blockFor,
                                         gotData,
                                         i,
                                         ranges.size());
                        }
                        throw ex;
                    } catch (DigestMismatchException e) {
                        throw new AssertionError(e); // no digests in range slices yet
                    }

                    // if we're done, great, otherwise, move to the next range
                    int count = countLiveRows ? liveRowCount : rows.size();
                    if (count >= rowsToBeFetched) {
                        haveSufficientRows = true;
                        break;
                    }
                }

                try {
                    FBUtilities.waitOnFutures(repairResponses, DatabaseDescriptor.getWriteRpcTimeout());
                } catch (TimeoutException ex) {
                    // We got all responses, but timed out while repairing
                    int blockFor = consistency_level.blockFor(keyspace);
                    if (Tracing.isTracing()) Tracing.trace(
                            "Timed out while read-repairing after receiving all {} data and digest responses",
                            blockFor);
                    else logger.debug(
                            "Range slice timeout while read-repairing after receiving all {} data and digest responses",
                            blockFor);
                    throw new ReadTimeoutException(consistency_level, blockFor - 1, blockFor, true);
                }

                if (haveSufficientRows)
                    return makeResult(rows, searcher, expressions, limit, rowsPerRange, rowKeys, searcher.mapper());

                // we didn't get enough rows in our concurrent fetch; recalculate our concurrency factor
                // based on the results we've seen so far (as long as we still have ranges left to query)
                if (i < ranges.size()) {
                    float fetchedRows = countLiveRows ? liveRowCount : rows.size();
                    float remainingRows = rowsToBeFetched - fetchedRows;
                    float actualRowsPerRange;
                    if (fetchedRows == 0.0) {
                        // we haven't actually gotten any results, so query all remaining ranges at once
                        actualRowsPerRange = 0.0f;
                        concurrencyFactor = ranges.size() - i;
                    } else {
                        actualRowsPerRange = fetchedRows / i;
                        concurrencyFactor = Math.max(1,
                                                     Math.min(ranges.size() - i,
                                                              Math.round(remainingRows / actualRowsPerRange)));
                    }
                    logger.debug(
                            "Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                            actualRowsPerRange,
                            (int) remainingRows,
                            concurrencyFactor);
                }
            }
        } finally {
            long latency = System.nanoTime() - startTime;
            rangeMetrics.addNano(latency);
            Keyspace.open(keyspaceName).getColumnFamilyStore(columnFamily).metric.coordinatorScanLatency.update(latency,
                                                                                                                TimeUnit.NANOSECONDS);
        }
        return makeResult(rows, searcher, expressions, limit, rowsPerRange, rowKeys, searcher.mapper());
    }

    public static Pair<List<Row>, RowKeys> makeResult(List<Row> rows,
                                                      IndexSearcher searcher,
                                                      List<IndexExpression> expressions,
                                                      int limit,
                                                      Map<AbstractBounds<RowPosition>, List<Row>> rowsPerRange,
                                                      RowKeys rowKeys,
                                                      RowMapper mapper) {
        rows = searcher.postReconciliationProcessing(expressions, rows);
        rows = rows.size() > limit ? rows.subList(0, limit) : rows;
        RowKeys newRowKeys = new RowKeys();
        for (Map.Entry<AbstractBounds<RowPosition>, List<Row>> entry : rowsPerRange.entrySet()) {
            RowKey rowKey = rowKey(entry.getKey(), rowKeys);
            RowKey newRowKey = last(mapper, rowKey, rows, entry.getValue());
            if (newRowKey != null) newRowKeys.add(newRowKey);
        }
        return Pair.create(rows, newRowKeys);
    }

}
