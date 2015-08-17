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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;

import java.nio.ByteBuffer;

/**
 * Class for several regular cells mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class RegularCellsMapper {

    /** The column family metadata. */
    private final CFMetaData metadata;

    /** The mapping schema. */
    private final Schema schema;

    /**
     * Builds a new {@link RegularCellsMapper} for the specified column family metadata and schema.
     *
     * @param metadata The column family metadata.
     * @param schema   A {@link Schema}.
     */
    private RegularCellsMapper(CFMetaData metadata, Schema schema) {
        this.metadata = metadata;
        this.schema = schema;
    }

    /**
     * Returns a new {@link RegularCellsMapper} for the specified column family metadata.
     *
     * @param metadata The column family metadata.
     * @param schema   A {@link Schema}.
     * @return A new {@link RegularCellsMapper} for the specified column family metadata.
     */
    public static RegularCellsMapper instance(CFMetaData metadata, Schema schema) {
        return new RegularCellsMapper(metadata, schema);
    }

    /**
     * Returns the columns contained in the regular cells specified row. Note that not all the contained columns are
     * returned, but only the regular cell ones.
     *
     * @param columnFamily A row column family.
     * @return The columns contained in the regular cells specified row.
     */
    public Columns columns(ColumnFamily columnFamily) {

        Columns columns = new Columns();

        // Stuff for grouping collection columns (sets, lists and maps)
        String name;
        CollectionType<?> collectionType;

        for (Cell cell : columnFamily) {

            CellName cellName = cell.name();
            name = cellName.cql3ColumnName(metadata).toString();
            if (!schema.maps(name)) {
                continue;
            }

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(cellName);
            if (columnDefinition == null) {
                continue;
            }

            AbstractType<?> valueType = columnDefinition.type;

            ByteBuffer cellValue = cell.value();

            if (valueType.isCollection()) {
                collectionType = (CollectionType<?>) valueType;
                switch (collectionType.kind) {
                    case SET: {
                        AbstractType<?> type = collectionType.nameComparator();
                        ByteBuffer value = cellName.collectionElement();
                        columns.add(Column.fromDecomposed(name, value, type, true));
                        break;
                    }
                    case LIST: {
                        AbstractType<?> type = collectionType.valueComparator();
                        columns.add(Column.fromDecomposed(name, cellValue, type, true));
                        break;
                    }
                    case MAP: {
                        AbstractType<?> type = collectionType.valueComparator();
                        ByteBuffer keyValue = cellName.collectionElement();
                        AbstractType<?> keyType = collectionType.nameComparator();
                        String nameSufix = keyType.compose(keyValue).toString();
                        columns.add(Column.fromDecomposed(name, nameSufix, cellValue, type, true));
                        break;
                    }
                }
            } else {
                columns.add(Column.fromDecomposed(name, cellValue, valueType, false));
            }
        }

        return columns;
    }
}
