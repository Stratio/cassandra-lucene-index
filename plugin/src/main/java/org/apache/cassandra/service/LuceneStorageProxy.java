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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.*;
import com.stratio.cassandra.lucene.Index;
import org.apache.cassandra.metrics.ClientRequestMetrics;

import java.lang.reflect.Method;
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

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel)
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
}
