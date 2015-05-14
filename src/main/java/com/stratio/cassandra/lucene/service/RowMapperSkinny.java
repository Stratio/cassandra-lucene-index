/*
 * Copyright 2014, Stratio.
 *
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
package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

/**
 * {@link RowMapper} for skinny rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowMapperSkinny extends RowMapper {

    /**
     * Builds a new {@link RowMapperSkinny} for the specified column family metadata, indexed column definition and
     * {@link Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapperSkinny(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        super(metadata, columnDefinition, schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Columns columns(Row row) {
        Columns columns = new Columns();
        columns.add(partitionKeyMapper.columns(row));
        columns.add(regularCellsMapper.columns(row));
        return columns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document document(Row row) {
        DecoratedKey partitionKey = row.key;
        Document document = new Document();
        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);
        schema.addFields(document, columns(row));
        return document;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sort sort() {
        return new Sort(tokenMapper.sortFields());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Query query(DataRange dataRange) {
        return tokenMapper.query(dataRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily) {
        return metadata.comparator.makeCellName(columnDefinition.name.bytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowComparator naturalComparator() {
        return new RowComparatorNatural();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchResult searchResult(Document document, ScoreDoc scoreDoc) {
        DecoratedKey partitionKey = partitionKeyMapper.partitionKey(document);
        return new SearchResult(partitionKey, null, scoreDoc);
    }
}
