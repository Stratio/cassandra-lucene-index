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

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Class for several regular cells mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RegularCellsMapper {

    /**
     * The column family metadata.
     */
    private final CFMetaData metadata;

    /**
     * Builds a new {@link RegularCellsMapper} for the specified column family metadata.
     *
     * @param metadata The column family metadata.
     */
    private RegularCellsMapper(CFMetaData metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns a new {@link RegularCellsMapper} for the specified column family metadata.
     *
     * @param metadata The column family metadata.
     * @return A new {@link RegularCellsMapper} for the specified column family metadata.
     */
    public static RegularCellsMapper instance(CFMetaData metadata) {
        return new RegularCellsMapper(metadata);
    }

    /**
     * Returns the columns contained in the regular cells specified {@link Row}. Note that not all the contained columns
     * are returned, but only the regular cell ones.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the regular cells specified {@link Row}.
     */
    @SuppressWarnings("rawtypes")
    public Columns columns(Row row) {

        ColumnFamily columnFamily = row.cf;
        Columns columns = new Columns();

        // Get row's columns iterator skipping clustering column
        Iterator<Cell> cellIterator = columnFamily.iterator();
        cellIterator.next();

        // Stuff for grouping collection columns (sets, lists and maps)
        String name;
        CollectionType collectionType;

        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            CellName cellName = cell.name();
            ColumnDefinition columnDefinition = metadata.getColumnDefinition(cellName);
            if (columnDefinition == null) {
                continue;
            }

            AbstractType<?> valueType = columnDefinition.type;

            ByteBuffer cellValue = cell.value();

            name = cellName.cql3ColumnName(metadata).toString();

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
