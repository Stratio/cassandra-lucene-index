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
import org.apache.cassandra.db.marshal.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Class for several regular cells mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class RegularCellsMapper {

    private static final Logger logger = LoggerFactory.getLogger(RegularCellsMapper.class);
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

    private Columns processUserTypeConstents(ByteBuffer name, AbstractType<?> type) {
        Columns columns = new Columns();

        return columns;
    }

    private Columns process(Cell cell, AbstractType<?> valueType) {
        Columns columns = new Columns();
        String name = cell.name().cql3ColumnName(metadata).toString();

        if (valueType.isCollection()) {
            CollectionType<?> collectionType = (CollectionType<?>) valueType;
            switch (collectionType.kind) {
                case SET: {
                    AbstractType<?> type = collectionType.nameComparator();
                    ByteBuffer value = cell.name().collectionElement();

                    columns.add(Column.fromDecomposed(name, value, type, true));
                    break;
                }
                case LIST: {
                    AbstractType<?> type = collectionType.valueComparator();
                    columns.add(Column.fromDecomposed(name, cell.value(), type, true));
                    break;
                }
                case MAP: {
                    AbstractType<?> type = collectionType.valueComparator();
                    ByteBuffer keyValue = cell.name().collectionElement();
                    AbstractType<?> keyType = collectionType.nameComparator();
                    String nameSuffix = keyType.compose(keyValue).toString();
                    columns.add(Column.fromDecomposed(name, nameSuffix, cell.value(), type, true));
                    break;
                }
            }
        } else if (valueType instanceof UserType) {
            logger.debug("Is a usertype ");
            UserType userType = (UserType) valueType;
            ByteBuffer[] values = userType.split(cell.value());
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                String fieldName = userType.fieldNameAsString(i);
                AbstractType<?> type = userType.fieldType(i);

                logger.debug("Is a usertype name: " +
                             fieldName +
                             " type:" +
                             type +
                             " value: " + type.getString(values[i]));

                columns.add(Column.fromDecomposed(name + "." + fieldName, values[i], type, true));
            }
        } else {
            columns.add(Column.fromDecomposed(name, cell.value(), valueType, false));
        }
        logger.debug("RegularCellMapper returnin process cell: "+columns.toString());
        return columns;
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
        logger.debug("RegularCellMaper: columns...");
        // Stuff for grouping collection columns (sets, lists and maps)
        String name;

        for (Cell cell : columnFamily) {
            logger.debug("RegularCellMaper: getting columns for cell: "+cell.name().toString());
            CellName cellName = cell.name();
            name = cellName.cql3ColumnName(metadata).toString();

            if (!schema.maps(name)) {
                logger.debug("RegularCellMaper: schema doesnot map that name:"+ name);
                continue;
            }

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(cellName);
            if (columnDefinition == null) {
                logger.debug("RegularCellMaper: there is no columnDefinition ");
                continue;
            }

            AbstractType<?> valueType = columnDefinition.type;
            logger.debug("RegularCellMaper: cell cql3 name: " + name + " ColumnDefinition Type: " + valueType.toString());

            columns.add(process(cell,valueType));

        }
        logger.debug("Columns : "+ columns.toString());
        return columns;
    }
}
