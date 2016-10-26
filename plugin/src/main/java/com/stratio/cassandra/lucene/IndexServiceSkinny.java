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

import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.column.ColumnsMapper;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.key.PartitionMapper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

import java.util.*;

import static org.apache.cassandra.db.PartitionPosition.Kind.*;

/**
 * {@link IndexService} for skinny rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class IndexServiceSkinny extends IndexService {

    /**
     * Constructor using the specified {@link IndexOptions}.
     *
     * @param table the indexed table
     * @param indexMetadata the index metadata
     */
    IndexServiceSkinny(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        super(table, indexMetadata);
        init();
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> fieldsToLoad() {
        return new HashSet<>(Collections.singletonList(PartitionMapper.FIELD_NAME));
    }

    /** {@inheritDoc} */
    @Override
    public List<SortField> keySortFields() {
        return Arrays.asList(tokenMapper.sortField(), partitionMapper.sortField());
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
        return new Columns().add(partitionMapper.columns(key)).add(ColumnsMapper.columns(row));
    }

    /** {@inheritDoc} */
    @Override
    protected List<IndexableField> keyIndexableFields(DecoratedKey key, Row row) {
        List<IndexableField> output= new ArrayList<>();
        output.addAll(tokenMapper.indexableFields(key));
        output.add(partitionMapper.indexableField(key));
        return output;
    }

    /** {@inheritDoc} */
    @Override
    public Term term(DecoratedKey key, Row row) {
        return term(key);
    }

    /** {@inheritDoc} */
    @Override
    public Query query(DecoratedKey key, ClusteringIndexFilter filter) {
        return new TermQuery(term(key));
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Query> query(DataRange dataRange) {
        PartitionPosition start = dataRange.startKey();
        PartitionPosition stop = dataRange.stopKey();
        if (start.kind() == ROW_KEY && stop.kind() == ROW_KEY && start.equals(stop)) {
            return Optional.of(partitionMapper.query((DecoratedKey) start));
        }
        return tokenMapper.query(start.getToken(),
                                 stop.getToken(),
                                 start.kind() == MIN_BOUND,
                                 stop.kind() == MAX_BOUND);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Query> after(DecoratedKey key, Clustering clustering) {
        return key == null ? Optional.empty() : Optional.of(partitionMapper.query(key));
    }

    /** {@inheritDoc} */
    @Override
    public IndexReaderSkinny indexReader(DocumentIterator documents, ReadCommand command, ReadOrderGroup orderGroup) {
        return new IndexReaderSkinny(this, command, table, orderGroup, documents);

    }
}
