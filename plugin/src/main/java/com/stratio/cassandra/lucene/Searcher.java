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

package com.stratio.cassandra.lucene;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Searcher implements org.apache.cassandra.index.Index.Searcher {

    private static final Logger logger = LoggerFactory.getLogger(Searcher.class);

    private final ReadCommand command;

    /**
     * Builds a new searcher for the specified {@link ReadCommand}.
     * @param command The read command being executed.
     */
    public Searcher(ReadCommand command) {
        this.command = command;
    }

    /**
     * @param orderGroup the collection of OpOrder.Groups which the ReadCommand is being performed under.
     * @return partitions from the base table matching the criteria of the search.
     */
    public UnfilteredPartitionIterator search(ReadOrderGroup orderGroup) {
        logger.debug("Searching {}", orderGroup);
        return null;
    }

}
