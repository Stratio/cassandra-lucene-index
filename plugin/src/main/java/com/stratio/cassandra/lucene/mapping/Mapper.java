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

package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.IndexOptions;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for several mappings between Cassandra and Lucene data models.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Mapper {

    private static final Logger logger = LoggerFactory.getLogger(Mapper.class);

    private final CFMetaData tableMetadata;
    private final Schema schema;
    private final TokenMapper tokenMapper;
    private final PartitionMapper partitionMapper;
    private final ClusteringMapper clusteringMapper;
    private final KeyMapper keyMapper;
    private final CellsMapper cellsMapper;

    /**
     * Builds a new {@link Mapper} for the specified {@link IndexOptions}.
     *
     * @param tableMetadata the indexed table metadata
     * @param schema the indexing schema
     */
    public Mapper(CFMetaData tableMetadata, Schema schema) {
        this.tableMetadata = tableMetadata;
        this.schema = schema;
        tokenMapper = new TokenMapper();
        partitionMapper = new PartitionMapper(tableMetadata);
        clusteringMapper = new ClusteringMapper(tableMetadata);
        keyMapper = new KeyMapper(partitionMapper, clusteringMapper);
        cellsMapper = new CellsMapper(tableMetadata);
    }

    /**
     * Returns a {@link Columns} representing the specified {@link Row}.
     *
     * @param key A partition key.
     * @param row A {@link Row}.
     * @return The columns representing the specified {@link Row}.
     */
    public Columns columns(DecoratedKey key, Row row) {
        Clustering clustering = row.clustering();
        Columns columns = new Columns();
        partitionMapper.addColumns(columns, key);
        clusteringMapper.addColumns(columns, clustering);
        cellsMapper.addColumns(columns, row);
        return columns;
    }

    /**
     * Returns a {@link Document} representing the specified {@link Row}.
     *
     * @param key A partition key.
     * @param row A {@link Row}.
     * @return The document representing the specified {@link Row}.
     */
    public Document document(DecoratedKey key, Row row) {
        Clustering clustering = row.clustering();
        Document document = new Document();
        tokenMapper.addFields(document, key);
        partitionMapper.addFields(document, key);
        clusteringMapper.addFields(document, clustering);
        keyMapper.addFields(document, key, clustering);
        schema.addFields(document, columns(key, row));
        return document;
    }

    public Term term(DecoratedKey key, Row row) {
        Clustering clustering = row.clustering();
        return keyMapper.term(key, clustering);
    }

    public Term term(DecoratedKey key) {
        return partitionMapper.term(key);
    }

}
