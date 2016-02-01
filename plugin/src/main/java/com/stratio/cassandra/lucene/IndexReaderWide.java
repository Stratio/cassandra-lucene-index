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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.lucene.document.Document;

import java.util.NavigableSet;

/**
 * {@link IndexReader} for wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexReaderWide extends IndexReader {

    private final IndexServiceWide service;
    private Document nextDoc;

    public IndexReaderWide(ReadCommand command,
                           ColumnFamilyStore table,
                           ReadExecutionController controller,
                           DocumentIterator documents,
                           IndexServiceWide service) {
        super(command, table, controller, documents);
        this.service = service;
    }

    @Override
    protected boolean prepareNext() {

        if (next != null) {
            return true;
        }

        if (nextDoc == null) {
            if (!documents.hasNext()) {
                return false;
            }
            nextDoc = documents.next();
        }

        NavigableSet<Clustering> clusterings = service.clusterings();
        DecoratedKey key = service.decoratedKey(nextDoc);

        while (nextDoc != null && key.getKey().equals(service.decoratedKey(nextDoc).getKey())) {
            Clustering clustering = service.clustering(nextDoc);
            if (command.selectsKey(key) && command.selectsClustering(key, clustering)) {
                clusterings.add(clustering);
            }
            if (!documents.hasNextWithoutFetch()) {
                break;
            }
            nextDoc = documents.hasNext() ? documents.next() : null;
        }

        if (clusterings.isEmpty()) {
            return prepareNext();
        }

        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        SinglePartitionReadCommand dataCommand = SinglePartitionReadCommand.create(table.metadata,
                                                                                   command.nowInSec(),
                                                                                   command.columnFilter(),
                                                                                   command.rowFilter(),
                                                                                   DataLimits.NONE,
                                                                                   key,
                                                                                   filter);
        UnfilteredRowIterator data = dataCommand.queryMemtableAndDisk(table, controller);

        if (data.isEmpty()) {
            data.close();
            return prepareNext();
        }

        next = data;
        return true;
    }
}
