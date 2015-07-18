package org.apache.cassandra.cql3;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.RowKey;
import com.stratio.cassandra.lucene.RowKeys;
import com.stratio.cassandra.lucene.service.RowMapper;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.service.RangeHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@SuppressWarnings("unchecked")
public class LuceneQueryProcessor {

    private static final Method getRestrictedRanges;
    private static final Method getLiveSortedEndpoints;
    private static final Method intersection;
    private static final Field isCount;

    static {
        try {
            Class clazz = StorageProxy.class;
            getRestrictedRanges = clazz.getDeclaredMethod("getRestrictedRanges", AbstractBounds.class);
            getRestrictedRanges.setAccessible(true);
            getLiveSortedEndpoints = clazz.getDeclaredMethod("getLiveSortedEndpoints",
                                                             Keyspace.class,
                                                             RingPosition.class);
            getLiveSortedEndpoints.setAccessible(true);
            intersection = clazz.getDeclaredMethod("intersection", List.class, List.class);
            intersection.setAccessible(true);
            isCount = SelectStatement.Parameters.class.getDeclaredField("isCount");
            isCount.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> range)
    throws Exception {
        return (List<AbstractBounds<T>>) getRestrictedRanges.invoke(StorageProxy.instance, range);
    }

    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition pos) throws Exception {
        return (List<InetAddress>) getLiveSortedEndpoints.invoke(StorageProxy.instance, keyspace, pos);
    }

    public static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2) throws Exception {
        return (List<InetAddress>) intersection.invoke(StorageProxy.instance, l1, l2);
    }

    private static boolean isCount(SelectStatement selectStatement) throws Exception {
        return (boolean) isCount.get(selectStatement.parameters);
    }

    public static List<AbstractBounds<RowPosition>> ranges(AbstractBounds<RowPosition> keyRange,
                                                           Keyspace keyspace,
                                                           ConsistencyLevel consistency_level) throws Exception {

        List<AbstractBounds<RowPosition>> result = new ArrayList<>();

        List<? extends AbstractBounds<RowPosition>> ranges;
        if (keyspace.getReplicationStrategy() instanceof LocalStrategy) ranges = keyRange.unwrap();
        else ranges = getRestrictedRanges(keyRange);

        int concurrencyFactor = ranges.size();

        int i = 0;
        AbstractBounds<RowPosition> nextRange = null;
        List<InetAddress> nextEndpoints = null;
        List<InetAddress> nextFilteredEndpoints = null;
        while (i < ranges.size()) {
            int concurrentFetchStartingIndex = i;
            while ((i - concurrentFetchStartingIndex) < concurrencyFactor) {
                AbstractBounds<RowPosition> range = nextRange == null ? ranges.get(i) : nextRange;
                List<InetAddress> liveEndpoints = nextEndpoints == null ?
                                                  getLiveSortedEndpoints(keyspace, range.right) :
                                                  nextEndpoints;
                List<InetAddress> filteredEndpoints = nextFilteredEndpoints == null ?
                                                      consistency_level.filterForQuery(keyspace, liveEndpoints) :
                                                      nextFilteredEndpoints;
                ++i;

                while (i < ranges.size()) {
                    nextRange = ranges.get(i);
                    nextEndpoints = getLiveSortedEndpoints(keyspace, nextRange.right);
                    nextFilteredEndpoints = consistency_level.filterForQuery(keyspace, nextEndpoints);

                    if (range.right.isMinimum()) break;

                    List<InetAddress> merged = intersection(liveEndpoints, nextEndpoints);

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

                result.add(range);
            }
        }

        return result;
    }

    public static ResultMessage.Rows run(IndexSearcher indexSearcher,
                                         String keyspaceName,
                                         String columnFamily,
                                         long timestamp,
                                         IDiskAtomFilter predicate,
                                         AbstractBounds<RowPosition> range,
                                         List<IndexExpression> expressions,
                                         int limit,
                                         ConsistencyLevel consistency_level,
                                         int pageSize,
                                         SelectStatement statement,
                                         QueryOptions options) throws Exception {

        RowMapper mapper = indexSearcher.mapper();
        PagingState pagingState = options.getPagingState();
        RowKeys rowKeys = null;
        if (pagingState != null) {
            limit = pagingState.remaining;
            ByteBuffer bb = pagingState.partitionKey;
            if (!ByteBufferUtils.isEmpty(bb)) {
                rowKeys = mapper.rowKeys(bb);
            }
        }

        Keyspace keyspace = Keyspace.open(keyspaceName);


        if (pageSize < 0) pageSize = limit;
        int remaining;
        boolean isCount = isCount(statement);

        List<? extends AbstractBounds<RowPosition>> ranges = ranges(range, keyspace, consistency_level);
        int numRanges = ranges.size();

        List<Row> rows = new ArrayList<>();
        List<Row> iterationRows;

        do {

            List<RangeHandler> rangeHandlers = new ArrayList<>(numRanges);
            for (AbstractBounds<RowPosition> subRange : ranges) {
                rangeHandlers.add(new RangeHandler(keyspace,
                                                   columnFamily,
                                                   timestamp,
                                                   predicate,
                                                   subRange,
                                                   expressions,
                                                   pageSize,
                                                   consistency_level,
                                                   rowKeys,
                                                   mapper).send());
            }

            iterationRows = new ArrayList<>();
            List<AsyncOneResponse> repairResults = new ArrayList<>();
            for (RangeHandler handler : rangeHandlers) {
                handler.get();
                iterationRows.addAll(handler.getRows());
                repairResults.addAll(handler.getRepairResults());
            }
            FBUtilities.waitOnFutures(repairResults, DatabaseDescriptor.getWriteRpcTimeout());
            iterationRows = indexSearcher.postReconciliationProcessing(expressions, iterationRows);
            if (iterationRows.size() > pageSize) iterationRows = iterationRows.subList(0, pageSize);

            for (Row row: iterationRows) {
                rows.add(row);
            }

            remaining = iterationRows.size() < pageSize ? 0 : limit - rows.size();

            if (remaining > 0) {
                rowKeys = new RowKeys();
                for (RangeHandler rangeHandler : rangeHandlers) {
                    RowKey last = rangeHandler.last(iterationRows);
                    if (last == null) continue;
                    rowKeys.add(last);
                }
            }
            Log.info("@@@ ITERATION ENDS WITH " + rows.size() + " COLLECTED ROWS AND REMAINING " + remaining);
            Log.info("@@@ NEXT ITERATION WILL START " + rowKeys);
            for (Row row : iterationRows) {
                Log.info("\t" + row.key);
            }

        } while (isCount && remaining > 0 && iterationRows.size() == pageSize);
        Log.info("@@@ COMMAND ENDS WITH " + rows.size() + " COLLECTED ROWS AND REMAINING " + remaining);
        Log.info("@@@ NEXT COMMAND WILL START " + rowKeys);

        ResultMessage.Rows msg = statement.processResults(rows, options, limit, timestamp);
        if (remaining > 0) {
            ByteBuffer bb = mapper.byteBuffer(rowKeys);
            pagingState = new PagingState(bb, null, remaining);
            msg.result.metadata.setHasMorePages(pagingState);
        }
        Log.info("---------------------------------------------------");

        return msg;
    }

}
