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

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * {@link IndexReader} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexReaderSkinny extends IndexReader {

    private final IndexServiceSkinny service;

    public IndexReaderSkinny(ReadCommand command,
                             ColumnFamilyStore table,
                             ReadExecutionController controller,
                             DocumentIterator documents,
                             IndexServiceSkinny service) {
        super(command, table, controller, documents);
        this.service = service;
    }

    @Override
    protected boolean prepareNext() {
        while (next == null && documents.hasNext()) {
            DecoratedKey key = service.decoratedKey(documents.next());
            ClusteringIndexFilter filter = command.clusteringIndexFilter(key);
            UnfilteredRowIterator data = read(key, filter);
            if (data != null) {
                if (data.isEmpty()) {
                    data.close();
                } else {
                    next = data;
                }
            }
        }
        return next != null;
    }
}
