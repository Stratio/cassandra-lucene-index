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
import com.stratio.cassandra.lucene.schema.column.ColumnBuilder;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Class for several static columns mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class StaticColumnsMapper {

    private final CFMetaData metadata;

    public StaticColumnsMapper(CFMetaData metadata) {
        this.metadata = metadata;
    }

    /**
     * Adds to the specified {@link Column} to the {@link Column}s contained in the specified {@link Row}.
     *
     * @param columns The {@link Columns} in which the {@link Column}s are going to be added.
     * @param row     A {@link Row}.
     */
    public void addColumns(Columns columns, Row row) {

        for (ColumnDefinition columnDefinition : metadata.partitionColumns()) {
            if (columnDefinition.isStatic()) {
                Cell cell = row.getCell(columnDefinition);
                if (cell != null) {
                    String name = columnDefinition.name.toString();
                    ByteBuffer value = cell.value();
                    AbstractType<?> type = columnDefinition.cellValueType();
                    columns.add(Column.builder(name).buildWithDecomposed(value, type));
                }
            }
        }
    }

}
