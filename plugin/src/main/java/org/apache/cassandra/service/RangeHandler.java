package org.apache.cassandra.service;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.RowKey;
import com.stratio.cassandra.lucene.RowKeys;
import com.stratio.cassandra.lucene.service.RowMapper;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalRangeSliceRunnable;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public final class RangeHandler {

    private AbstractBounds<RowPosition> range;
    private ReadCallback<RangeSliceReply, Iterable<Row>> callback;
    private List<Row> rows = new ArrayList<>();
    private List<AsyncOneResponse> repairResults;
    RowMapper mapper;
    RowKey after;

    public RangeHandler(Keyspace ks,
                        String cf,
                        long timestamp,
                        IDiskAtomFilter predicate,
                        AbstractBounds<RowPosition> range,
                        List<IndexExpression> filter,
                        int limit,
                        ConsistencyLevel consistency,
                        RowKeys rowKeys,
                        RowMapper mapper) throws Exception {
        this.rows = new ArrayList<>();
        this.range = range;
        this.mapper = mapper;

        after = rowKey(range, rowKeys);
        // Log.info("@@@ QUERYING TO " + range + " FOR " + limit + " AFTER " + after);

        List<IndexExpression> decoratedFilter = new ArrayList<>(filter);
        if (after != null) {
            decoratedFilter.add(new IndexExpression(IndexSearcher.AFTER, Operator.EQ, mapper.byteBuffer(after)));
        }
        RangeSliceCommand command = new RangeSliceCommand(ks.getName(),
                                                          cf,
                                                          timestamp,
                                                          predicate,
                                                          range,
                                                          decoratedFilter,
                                                          limit);

        List<InetAddress> liveEndpoints = LuceneQueryProcessor.getLiveSortedEndpoints(ks, range.right);
        List<InetAddress> filteredEndpoints = consistency.filterForQuery(ks, liveEndpoints);

        // collect replies and resolve according to consistency level
        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, timestamp);
        int highEndpoint = Math.min(filteredEndpoints.size(), consistency.blockFor(ks));
        List<InetAddress> minimalEndpoints = filteredEndpoints.subList(0, highEndpoint);
        callback = new ReadCallback<>(resolver, consistency, command, minimalEndpoints);
        callback.assureSufficientLiveNodes();
        resolver.setSources(filteredEndpoints);

        if (filteredEndpoints.size() == 1 && filteredEndpoints.get(0).equals(FBUtilities.getBroadcastAddress())) {
            LocalRangeSliceRunnable localRunnable = new StorageProxy.LocalRangeSliceRunnable(command, callback);
            StageManager.getStage(Stage.READ).execute(localRunnable, Tracing.instance.get());
        } else {
            MessageOut<? extends AbstractRangeCommand> message = command.createMessage();
            for (InetAddress endpoint : filteredEndpoints) {
                Tracing.trace("Enqueuing request to {}", endpoint);
                MessagingService.instance().sendRR(message, endpoint, callback);
            }
        }
    }

    private RowKey rowKey(AbstractBounds<RowPosition> range, RowKeys rowKeys) {
        if (rowKeys == null) return null;
        for (RowKey rowKey : rowKeys) {
            DecoratedKey key = rowKey.getPartitionKey();
            if (range.contains(key)) return rowKey;
        }
        return null;
    }

    public RangeHandler get() throws ReadTimeoutException {
        RangeSliceResponseResolver resolver = (RangeSliceResponseResolver) callback.resolver;
        try {
            for (Row row : callback.get()) {
                rows.add(row);
            }
            repairResults = resolver.repairResults;
        } catch (DigestMismatchException e) {
            throw new AssertionError(e); // no digests in range slices yet
        }
        return this;
    }

    public List<Row> getRows() {
        // Log.info("@@@ QUERY TO " + range + " FOUNDS " + rows.size());
        for (Row row : rows) {
            Log.info("\t" + row.key);
        }
        return rows;
    }

    public List<AsyncOneResponse> getRepairResults() {
        return repairResults;
    }

    public RowKey last(List<Row> results) {
        for (int i = rows.size() - 1; i >= 0; i--) {
            Row row = rows.get(i);
            if (results.contains(row)) return mapper.rowKey(row);
        }
        return after;
    }
}
