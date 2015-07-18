package org.apache.cassandra.service;

import com.stratio.cassandra.lucene.IndexSearcher;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.cql3.LuceneQueryProcessor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalRangeSliceRunnable;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public final class RangeHandler {

    private Keyspace ks;
    private String cf;
    private long timestamp;
    private ConsistencyLevel consistency;
    private AbstractBounds<RowPosition> range;
    private ReadCallback<RangeSliceReply, Iterable<Row>> callback;
    private List<Row> rows = new ArrayList<>();
    private List<AsyncOneResponse> repairResults;
    private int limit;
    private IDiskAtomFilter predicate;
    private List<IndexExpression> expressions;
    private RangeSliceCommand command;

    public RangeHandler(Keyspace ks,
                        String cf,
                        long timestamp,
                        IDiskAtomFilter predicate,
                        AbstractBounds<RowPosition> range,
                        List<IndexExpression> filter,
                        int limit,
                        ConsistencyLevel consistency,
                        Row after) throws Exception {
        this.cf = cf;
        this.ks = ks;
        this.timestamp = timestamp;
        this.consistency = consistency;
        this.rows = new ArrayList<>();
        this.range = range;
        this.limit = limit;
        this.predicate = predicate;
        this.expressions = filter;

        List<IndexExpression> decoratedFilter = decoratedFilter(filter, after);
        command = new RangeSliceCommand(ks.getName(), cf, timestamp, predicate, range, decoratedFilter, limit);
    }

    public RangeHandler send() throws Exception {

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
        return this;
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
        return rows;
    }

    public List<AsyncOneResponse> getRepairResults() {
        return repairResults;
    }

    public RangeHandler withStart(List<Row> results) throws Exception {
        Row last = last(results);
        return new RangeHandler(ks, cf, timestamp, predicate, range, expressions, limit, consistency, last);
    }

    private Row last(List<Row> results) {
        for (int i = rows.size() - 1; i >= 0; i--) {
            Row row = rows.get(i);
            if (results.contains(row)) return row;
        }
        return null;
    }

    private List<IndexExpression> decoratedFilter(List<IndexExpression> filter, Row after) throws IOException {
        if (after == null) return filter;
        List<IndexExpression> decoratedFilter = new ArrayList<>(filter);
        int size = (int) Row.serializer.serializedSize(after, MessagingService.current_version);
        DataOutputBuffer dob = new DataOutputBuffer(size);
        Row.serializer.serialize(after, dob, MessagingService.current_version);
        ByteBuffer value = dob.asByteBuffer();
        decoratedFilter.add(new IndexExpression(IndexSearcher.AFTER, Operator.EQ, value));
        return decoratedFilter;
    }
}
