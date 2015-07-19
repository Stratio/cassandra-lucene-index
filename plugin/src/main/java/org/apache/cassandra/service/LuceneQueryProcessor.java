package org.apache.cassandra.service;

import com.stratio.cassandra.lucene.IndexSearcher;
import com.stratio.cassandra.lucene.RowKey;
import com.stratio.cassandra.lucene.RowKeys;
import com.stratio.cassandra.lucene.service.RowMapper;
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
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
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

    public static Pair<List<Row>, RowKeys> run(IndexSearcher indexSearcher,
                                               String keyspaceName,
                                               String columnFamily,
                                               long timestamp,
                                               IDiskAtomFilter predicate,
                                               AbstractBounds<RowPosition> range,
                                               List<IndexExpression> expressions,
                                               int limit,
                                               ConsistencyLevel consistency_level,
                                               RowKeys rowKeys) throws Exception {

        RowMapper mapper = indexSearcher.mapper();

        Keyspace keyspace = Keyspace.open(keyspaceName);

        List<? extends AbstractBounds<RowPosition>> ranges = getRestrictedRanges(range);
        int numRanges = ranges.size();

        List<RangeHandler> rangeHandlers = new ArrayList<>(numRanges);
        for (AbstractBounds<RowPosition> subRange : ranges) {
            rangeHandlers.add(new RangeHandler(keyspace,
                                               columnFamily,
                                               timestamp,
                                               predicate,
                                               subRange,
                                               expressions,
                                               limit,
                                               consistency_level,
                                               rowKeys,
                                               mapper));
        }

        List<Row> rows = new ArrayList<>();
        List<AsyncOneResponse> repairResults = new ArrayList<>();
        for (RangeHandler handler : rangeHandlers) {
            handler.get();
            rows.addAll(handler.getRows());
            repairResults.addAll(handler.getRepairResults());
        }

        FBUtilities.waitOnFutures(repairResults, DatabaseDescriptor.getWriteRpcTimeout());
        rows = indexSearcher.postReconciliationProcessing(expressions, rows);
        if (rows.size() > limit) rows = rows.subList(0, limit);

        rowKeys = new RowKeys();
        for (RangeHandler rangeHandler : rangeHandlers) {
            RowKey last = rangeHandler.last(rows);
            if (last == null) continue;
            rowKeys.add(last);
        }

        return Pair.create(rows, rowKeys);
    }

}
