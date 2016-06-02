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
package org.apache.cassandra.service;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.stratio.cassandra.lucene.Index;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.utils.AbstractIterator;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Modified version of Apache Cassandra {@link StorageProxy} to be used with Lucene searches.
 */
public class LuceneStorageProxy {

    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");

    private static Method systemKeyspaceQuery;
    private static Method fetchRows;

    static {
        try {
            systemKeyspaceQuery = StorageProxy.class.getDeclaredMethod("systemKeyspaceQuery", List.class);
            systemKeyspaceQuery.setAccessible(true);
            fetchRows = StorageProxy.class.getDeclaredMethod("fetchRows", List.class, ConsistencyLevel.class);
            fetchRows.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds) throws ReflectiveOperationException {
        return (boolean) systemKeyspaceQuery.invoke(null, cmds);
    }

    private static PartitionIterator fetchRows(List<SinglePartitionReadCommand> commands, ConsistencyLevel cl)
    throws ReflectiveOperationException {
        return (PartitionIterator) fetchRows.invoke(null, commands, cl);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group,
                                         ConsistencyLevel consistencyLevel)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException,
           InvalidRequestException, ReflectiveOperationException {

        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(group.commands)) {
            readMetrics.unavailables.mark();
            throw new IsBootstrappingException();
        }

        return readRegular(group, consistencyLevel);
    }

    private static PartitionIterator readRegular(SinglePartitionReadCommand.Group group,
                                                 ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadFailureException, ReadTimeoutException, ReflectiveOperationException {
        long start = System.nanoTime();
        try {
            PartitionIterator result = fetchRows(group.commands, consistencyLevel);
            // If we have more than one command, then despite each read command honoring the limit, the total result
            // might not honor it and so we should enforce it
            if (group.commands.size() > 1) {
                ReadCommand command = group.commands.get(0);
                CFMetaData metadata = group.metadata();
                ColumnFamilyStore cfs = Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName);
                Index index = (Index) command.getIndex(cfs);
                result = index.postProcessorFor(group).apply(result, group);
                result = group.limits().filter(result, group.nowInSec());
            }

            return result;

        } catch (UnavailableException e) {
            readMetrics.unavailables.mark();
            throw e;
        } catch (ReadTimeoutException e) {
            readMetrics.timeouts.mark();
            throw e;
        } catch (ReadFailureException e) {
            readMetrics.failures.mark();
            throw e;
        } finally {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : group.commands) {
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency,
                                                                                                  TimeUnit.NANOSECONDS);
            }
        }
    }

    ///////////////////////////////////////

    public static RangeMerger rangeMerger(PartitionRangeReadCommand command, ConsistencyLevel consistency) {
        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        RangeIterator rangeIterator = new RangeIterator(command, keyspace, consistency);
        return new RangeMerger(rangeIterator, keyspace, consistency);
    }

    public static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange) {
        return StorageProxy.getRestrictedRanges(queryRange);
    }

    public static class RangeIterator extends AbstractIterator<RangeForQuery> {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
        private final int rangeCount;

        public RangeIterator(PartitionRangeReadCommand command, Keyspace keyspace, ConsistencyLevel consistency) {
            this.keyspace = keyspace;
            this.consistency = consistency;

            List<? extends AbstractBounds<PartitionPosition>>
                    l
                    = keyspace.getReplicationStrategy() instanceof LocalStrategy
                      ? command.dataRange().keyRange().unwrap()
                      : getRestrictedRanges(command.dataRange().keyRange());
            this.ranges = l.iterator();
            this.rangeCount = l.size();
        }

        public int rangeCount() {
            return rangeCount;
        }

        protected RangeForQuery computeNext() {
            if (!ranges.hasNext()) {
                return endOfData();
            }

            AbstractBounds<PartitionPosition> range = ranges.next();
            List<InetAddress> liveEndpoints = StorageProxy.getLiveSortedEndpoints(keyspace, range.right);
            return new RangeForQuery(range, liveEndpoints, consistency.filterForQuery(keyspace, liveEndpoints));
        }
    }

    public static class RangeForQuery {
        public final AbstractBounds<PartitionPosition> range;
        public final List<InetAddress> liveEndpoints;
        public final List<InetAddress> filteredEndpoints;

        public RangeForQuery(AbstractBounds<PartitionPosition> range,
                             List<InetAddress> liveEndpoints,
                             List<InetAddress> filteredEndpoints) {
            this.range = range;
            this.liveEndpoints = liveEndpoints;
            this.filteredEndpoints = filteredEndpoints;
        }
    }

    public static class RangeMerger extends AbstractIterator<RangeForQuery> {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final PeekingIterator<RangeForQuery> ranges;

        private RangeMerger(Iterator<RangeForQuery> iterator, Keyspace keyspace, ConsistencyLevel consistency) {
            this.keyspace = keyspace;
            this.consistency = consistency;
            this.ranges = Iterators.peekingIterator(iterator);
        }

        protected RangeForQuery computeNext() {
            if (!ranges.hasNext()) {
                return endOfData();
            }

            RangeForQuery current = ranges.next();

            // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
            // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
            // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
            while (ranges.hasNext()) {
                // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                // don't know how to deal with a wrapping range.
                // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                // wire compatibility, so It's likely easier not to bother;
                if (current.range.right.isMinimum()) {
                    break;
                }

                RangeForQuery next = ranges.peek();

                List<InetAddress> merged = intersection(current.liveEndpoints, next.liveEndpoints);

                // Check if there is enough endpoint for the merge to be possible.
                if (!consistency.isSufficientLiveNodes(keyspace, merged)) {
                    break;
                }

                List<InetAddress> filteredMerged = consistency.filterForQuery(keyspace, merged);

                // Estimate whether merging will be a win or not
                if (!DatabaseDescriptor.getEndpointSnitch()
                                       .isWorthMergingForRangeQuery(filteredMerged,
                                                                    current.filteredEndpoints,
                                                                    next.filteredEndpoints)) {
                    break;
                }

                // If we get there, merge this range and the next one
                current = new RangeForQuery(current.range.withNewRight(next.range.right), merged, filteredMerged);
                ranges.next(); // consume the range we just merged since we've only peeked so far
            }
            return current;
        }
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2) {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

}
