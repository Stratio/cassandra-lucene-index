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

import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.Optional;

/**
 * {@link IndexService} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexServiceSkinny extends IndexService {

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    protected IndexServiceSkinny(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public IndexWriterSkinny indexWriter(DecoratedKey key,
                                         int nowInSec,
                                         OpOrder.Group opGroup,
                                         IndexTransaction.Type transactionType) {
        return new IndexWriterSkinny(this, key, nowInSec, opGroup, transactionType);
    }

    /** {@inheritDoc} */
    @Override
    public Columns columns(DecoratedKey key, Row row) {
        Columns columns = new Columns();
        partitionMapper.addColumns(columns, key);
        columnsMapper.addColumns(columns, row);
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Document> document(DecoratedKey key, Row row) {
        Document document = new Document();
        Columns columns = columns(key, row);
        schema.addFields(document, columns);
        if (document.getFields().isEmpty()) {
            return Optional.empty();
        } else {
            tokenMapper.addFields(document, key);
            partitionMapper.addFields(document, key);
            return Optional.of(document);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Term term(DecoratedKey key, Row row) {
        return partitionMapper.term(key);
    }

    /** {@inheritDoc} */
    @Override
    public Term term(Document document) {
        return partitionMapper.term(document);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DecoratedKey key, ClusteringIndexFilter clusteringFilter) {
        return new TermQuery(term(key));
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DataRange dataRange) {
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        boolean includeStart = tokenMapper.includeStart(startPosition);
        boolean includeStop = tokenMapper.includeStop(stopPosition);
        return tokenMapper.query(startToken, stopToken, includeStart, includeStop);
    }

    /** {@inheritDoc} */
    @Override
    public IndexReaderSkinny indexReader(DocumentIterator documents,
                                         ReadCommand command,
                                         ReadOrderGroup orderGroup) {
        return new IndexReaderSkinny(command, table, orderGroup, documents, this);

    }
}
