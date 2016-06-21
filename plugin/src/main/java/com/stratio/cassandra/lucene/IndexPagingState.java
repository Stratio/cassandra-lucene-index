/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import com.stratio.cassandra.lucene.util.SimplePartitionIterator;
import com.stratio.cassandra.lucene.util.SimpleRowIterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.service.LuceneStorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.utils.Pair;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;

import static com.stratio.cassandra.lucene.util.ByteBufferUtils.compose;
import static com.stratio.cassandra.lucene.util.ByteBufferUtils.decompose;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.SinglePartitionReadCommand.Group;
import static org.apache.cassandra.db.filter.RowFilter.Expression;
import static org.apache.cassandra.service.LuceneStorageProxy.RangeMerger;
import static org.apache.cassandra.utils.ByteBufferUtil.readShortLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeShortLength;

/**
 * The paging state of a CQL query using Lucene. It tracks the primary keys of the last seen rows for each internal read
 * command of a CQL query. It also keeps the count of the remaining rows. This state can be serialized to be attached to
 * a CQL {@link PagingState} and/or to a search predicate.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexPagingState {

    /** The number of remaining rows. */
    private int remaining;

    /** If there could be more results. */
    private boolean hasMorePages = true;

    /** The last row positions */
    private final Map<DecoratedKey, Clustering> entries;

    /**
     * Constructor taking the remaining rows.
     *
     * @param remaining the number of remaining rows to be retrieved
     */
    private IndexPagingState(int remaining) {
        this.remaining = remaining;
        this.entries = new LinkedHashMap<>();
    }

    /**
     * Returns the number of remaining rows.
     *
     * @return the number of remaining rows
     */
    int remaining() {
        return remaining;
    }

    private void clear(AbstractBounds<PartitionPosition> bounds) {
        List<DecoratedKey> keysToRemove = new LinkedList<>();
        for (Map.Entry<DecoratedKey, Clustering> entry : entries.entrySet()) {
            DecoratedKey key = entry.getKey();
            if (bounds.contains(key)) {
                keysToRemove.add(key);
            }
        }
        keysToRemove.forEach(entries::remove);
    }

    /**
     * Returns the primary key of the last seen row for the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the primary key of the last seen row for {@code command}
     */
    Pair<DecoratedKey, Clustering> forCommand(ReadCommand command) {
        if (command instanceof SinglePartitionReadCommand) {
            return forCommand((SinglePartitionReadCommand) command);
        } else if (command instanceof PartitionRangeReadCommand) {
            return forCommand((PartitionRangeReadCommand) command);
        } else {
            throw new IndexException("Unsupported read command type: {}", command.getClass());
        }
    }

    private Pair<DecoratedKey, Clustering> forCommand(SinglePartitionReadCommand command) {
        DecoratedKey key = command.partitionKey();
        Clustering clustering = entries.get(key);
        if (clustering == null) {
            return null;
        }
        return Pair.create(key, clustering);
    }

    private Pair<DecoratedKey, Clustering> forCommand(PartitionRangeReadCommand command) {
        DataRange dataRange = command.dataRange();
        for (Map.Entry<DecoratedKey, Clustering> entry : entries.entrySet()) {
            DecoratedKey key = entry.getKey();
            if (dataRange.contains(key)) {
                Clustering clustering = entry.getValue();
                return Pair.create(key, clustering);
            }
        }
        return null;
    }

    /**
     * Adds this paging state to the specified {@link ReadQuery}.
     *
     * @param query a CQL query using the Lucene index
     * @throws ReflectiveOperationException
     */
    void rewrite(ReadQuery query) throws ReflectiveOperationException {
        if (query instanceof Group) {
            rewrite((Group) query);
        } else if (query instanceof ReadCommand) {
            rewrite((ReadCommand) query);
        } else {
            throw new IndexException("Unsupported query type {}", query.getClass());
        }
    }

    private void rewrite(ReadCommand command) throws ReflectiveOperationException {

        Field field = Expression.class.getDeclaredField("value");
        field.setAccessible(true);

        Expression expression = expression(command);

        ByteBuffer value = (ByteBuffer) field.get(expression);
        SearchBuilder searchBuilder = SearchBuilder.fromJson(UTF8Type.instance.compose(value));
        searchBuilder.paging(this);
        ByteBuffer newValue = UTF8Type.instance.decompose(searchBuilder.toJson());
        field.set(expression, newValue);
    }

    private Expression expression(ReadCommand command) throws ReflectiveOperationException {

        // Try with custom expressions
        for (Expression expression : command.rowFilter().getExpressions()) {
            if (expression.isCustom()) {
                return expression;
            }
        }

        // Try with dummy column
        ColumnFamilyStore cfs = Keyspace.open(command.metadata().ksName)
                                        .getColumnFamilyStore(command.metadata().cfName);
        for (Expression expression : command.rowFilter().getExpressions()) {
            for (org.apache.cassandra.index.Index index : cfs.indexManager.listIndexes()) {
                if (index instanceof Index && index.supportsExpression(expression.column(), expression.operator())) {
                    return expression;
                }
            }
        }

        return null;
    }

    private void rewrite(Group group) throws ReflectiveOperationException {
        for (SinglePartitionReadCommand command : group.commands) {
            rewrite(command);
        }
    }

    /**
     * Returns a CQL {@link PagingState} containing this Lucene paging state.
     *
     * @return a CQL paging state
     */
    PagingState toPagingState() {
        return hasMorePages ? new PagingState(toByteBuffer(), null, remaining, remaining) : null;
    }

    /**
     * Returns the Lucene paging state contained in the specified CQL {@link PagingState}. If the specified paging state
     * is {@code null}, then an empty Lucene paging state will be returned.
     *
     * @param state a CQL paging state
     * @param limit the query user limit
     * @return a Lucene paging state
     */
    static IndexPagingState build(PagingState state, int limit) {
        return state == null ? new IndexPagingState(limit) : build(state.partitionKey);
    }

    /**
     * Updates this paging state with the results of the specified query.
     *
     * @param query the query
     * @param data the results of {@code query}
     * @param consistency the query consistency level
     * @return a copy of the query results
     */
    PartitionIterator update(ReadQuery query, PartitionIterator data, ConsistencyLevel consistency) {
        if (query instanceof SinglePartitionReadCommand.Group) {
            return update((SinglePartitionReadCommand.Group) query, data);
        } else if (query instanceof PartitionRangeReadCommand) {
            return update((PartitionRangeReadCommand) query, data, consistency);
        } else {
            throw new IndexException("Unsupported query type {}", query.getClass());
        }
    }

    private PartitionIterator update(Group group, PartitionIterator data) {
        List<SimpleRowIterator> rowIterators = new LinkedList<>();

        int count = 0;
        while (data.hasNext()) {
            RowIterator partition = data.next();
            DecoratedKey key = partition.partitionKey();
            while (partition.hasNext()) {
                SimpleRowIterator newRowIterator = new SimpleRowIterator(partition);
                rowIterators.add(newRowIterator);
                Clustering clustering = newRowIterator.getRow().clustering();
                entries.put(key, clustering);
                if (remaining > 0) {
                    remaining--;
                }
                count++;
            }
            partition.close();
        }
        data.close();

        hasMorePages = remaining > 0 && count >= group.limits().count();

        return new SimplePartitionIterator(rowIterators);
    }

    private PartitionIterator update(PartitionRangeReadCommand command, PartitionIterator data, ConsistencyLevel cl) {

        // Collect query bounds
        RangeMerger rangeMerger = LuceneStorageProxy.rangeMerger(command, cl);
        List<AbstractBounds<PartitionPosition>> bounds = new LinkedList<>();
        while (rangeMerger.hasNext()) {
            bounds.add(rangeMerger.next().range);
        }

        List<SimpleRowIterator> rowIterators = new LinkedList<>();
        int count = 0;
        while (data.hasNext()) {
            RowIterator partition = data.next();
            DecoratedKey key = partition.partitionKey();
            AbstractBounds<PartitionPosition> bound = bounds.stream()
                                                            .filter(b -> b.contains(key))
                                                            .findAny()
                                                            .orElseGet(null);
            while (partition.hasNext()) {
                clear(bound);
                SimpleRowIterator newRowIterator = new SimpleRowIterator(partition);
                rowIterators.add(newRowIterator);
                Clustering clustering = newRowIterator.getRow().clustering();
                entries.put(key, clustering);
                if (remaining > 0) {
                    remaining--;
                }
                count++;
            }
            partition.close();
        }
        data.close();

        hasMorePages = remaining > 0 && count >= command.limits().count();

        return new SimplePartitionIterator(rowIterators);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("remaining", remaining).add("entries", entries).toString();
    }

    /**
     * Returns a byte buffer representation of this. Thew returned result can be read with {@link #build(ByteBuffer)}.
     *
     * @return a byte buffer representing this
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer[] entryValues = new ByteBuffer[entries.size()];
        entries.entrySet().stream().map(entry -> {
            DecoratedKey key = entry.getKey();
            Clustering clustering = entry.getValue();
            ByteBuffer[] clusteringValues = clustering.getRawValues();
            ByteBuffer[] values = new ByteBuffer[1 + clusteringValues.length];
            values[0] = key.getKey();
            System.arraycopy(clusteringValues, 0, values, 1, clusteringValues.length);
            return compose(values);
        }).collect(toList()).toArray(entryValues);
        ByteBuffer values = compose(entryValues);
        ByteBuffer out = ByteBuffer.allocate(2 + values.remaining());
        writeShortLength(out, remaining);
        out.put(values);
        out.flip();
        return out;
    }

    /**
     * Returns the paging state represented by the specified byte buffer, which should have been generated with {@link
     * #toByteBuffer()}.
     *
     * @param bb a byte buffer generated by {@link #toByteBuffer()}
     * @return the paging state reperesented by {@code bb}
     */
    public static IndexPagingState build(ByteBuffer bb) {
        int remaining = readShortLength(bb);
        IndexPagingState state = new IndexPagingState(remaining);
        Arrays.asList(decompose(bb))
              .stream()
              .forEach(bbe -> {
                  ByteBuffer[] values = decompose(bbe);
                  DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(values[0]);
                  Clustering clustering = new Clustering(Arrays.copyOfRange(values, 1, values.length));
                  state.entries.put(key, clustering);
              });
        return state;
    }
}
