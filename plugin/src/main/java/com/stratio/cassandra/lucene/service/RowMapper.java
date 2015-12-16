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

package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for several mappings between Cassandra and Lucene data models.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowMapper {

    protected static final Logger logger = LoggerFactory.getLogger(RowMapper.class);

    protected final CFMetaData tableMetadata;
    protected final Schema schema;
    protected final PartitionKeyMapper partitionKeyMapper;
    protected final ClusteringKeyMapper clusteringKeyMapper;
    protected final RegularColumnsMapper regularColumnsMapper;
    protected final StaticColumnsMapper staticColumnsMapper;

    /**
     * Builds a new {@link RowMapper} for the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     */
    protected RowMapper(IndexConfig config) {
        tableMetadata = config.getTableMetadata();
        schema = config.getSchema();
        partitionKeyMapper = new PartitionKeyMapper(tableMetadata);
        clusteringKeyMapper = new ClusteringKeyMapper(tableMetadata);
        regularColumnsMapper = new RegularColumnsMapper(tableMetadata);
        staticColumnsMapper = new StaticColumnsMapper(tableMetadata);
    }

    public Columns columns(DecoratedKey key, Row row) {
        Columns columns = new Columns();
        partitionKeyMapper.addColumns(columns, key);
        staticColumnsMapper.addColumns(columns, row);
        clusteringKeyMapper.addColumns(columns, row);
        regularColumnsMapper.addColumns(columns, row);
        return columns;
    }

}
