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

import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;

import java.nio.ByteBuffer;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class ClusteringKeyMapper {

    private final CFMetaData metadata;

    public ClusteringKeyMapper(CFMetaData metadata) {
        this.metadata = metadata;
    }

    /**
     * Adds the {@link Column}s contained in the specified {@link Row} to the specified {@link Column}s .
     *
     * @param columns The {@link Columns} in which the {@link Row} {@link Column}s are going to be added.
     * @param row     The {@link Row} which {@link Column}s are going to be added.
     */
    public void addColumns(Columns columns, Row row) {
        if (!row.isStatic()) {
            Clustering clustering = row.clustering();
            for (ColumnDefinition columnDefinition : metadata.clusteringColumns()) {
                String name = columnDefinition.name.toString();
                int position = columnDefinition.position();
                ByteBuffer value = clustering.get(position);
                AbstractType<?> type = columnDefinition.cellValueType();
                columns.add(Column.builder(name).buildWithDecomposed(value, type));
            }
        }
    }

}
