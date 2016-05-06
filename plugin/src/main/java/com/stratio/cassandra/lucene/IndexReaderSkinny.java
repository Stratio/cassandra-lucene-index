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

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

/**
 * {@link IndexReader} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class IndexReaderSkinny extends IndexReader {

    private final IndexServiceSkinny service;

    /**
     * Constructor taking the Cassandra read data and the Lucene results iterator.
     *
     * @param service the index service
     * @param command the read command
     * @param table the base table
     * @param orderGroup the order group of the read operation
     * @param documents the documents iterator
     */
    IndexReaderSkinny(IndexServiceSkinny service,
                      ReadCommand command,
                      ColumnFamilyStore table,
                      ReadOrderGroup orderGroup,
                      DocumentIterator documents) {
        super(command, table, orderGroup, documents);
        this.service = service;
    }

    @Override
    protected boolean prepareNext() {
        while (next == null && documents.hasNext()) {
            Pair<Document, ScoreDoc> nextDoc = documents.next();
            DecoratedKey key = service.decoratedKey(nextDoc.left);
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
