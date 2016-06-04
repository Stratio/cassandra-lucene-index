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
package com.stratio.cassandra.lucene.column;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

/**
 * Class for several regular column mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnsMapper {

    /**
     * Adds to the specified {@link Column} to the {@link Column}s contained in the specified {@link Row}.
     *
     * @param columns the {@link Columns} in which the {@link Column}s are going to be added
     * @param row aA {@link Row}
     */
    public void addColumns(Columns columns, Row row) {
        for (ColumnDefinition columnDefinition : row.columns()) {
            if (columnDefinition.isComplex()) {
                addColumns(columns, row.getComplexColumnData(columnDefinition));
            } else {
                addColumns(columns, row.getCell(columnDefinition));
            }
        }
    }

    private void addColumns(Columns columns, ComplexColumnData complexColumnData) {
        if (complexColumnData != null) {
            for (Cell cell : complexColumnData) {
                addColumns(columns, cell);
            }
        }
    }

    private void addColumns(Columns columns, Cell cell) {
        if (cell != null) {
            boolean isTombstone = cell.isTombstone();
            ColumnDefinition columnDefinition = cell.column();
            String name = columnDefinition.name.toString();
            AbstractType<?> type = cell.column().type;
            ByteBuffer value = cell.value();
            ColumnBuilder builder = Column.builder(name, cell.localDeletionTime());
            if (type.isCollection() && !type.isFrozenCollection()) {
                CollectionType<?> collectionType = (CollectionType<?>) type;
                switch (collectionType.kind) {
                    case SET: {
                        type = collectionType.nameComparator();
                        value = cell.path().get(0);
                        addColumns(isTombstone, columns, builder, type, value);
                        break;
                    }
                    case LIST: {
                        type = collectionType.valueComparator();
                        addColumns(isTombstone, columns, builder, type, value);
                        break;
                    }
                    case MAP: {
                        type = collectionType.valueComparator();
                        ByteBuffer keyValue = cell.path().get(0);
                        AbstractType<?> keyType = collectionType.nameComparator();
                        String nameSuffix = keyType.compose(keyValue).toString();
                        addColumns(isTombstone, columns, builder.withMapName(nameSuffix), type, value);
                        break;
                    }
                    default: {
                        throw new IndexException("Unknown collection type %s", collectionType.kind);
                    }
                }
            } else {
                addColumns(isTombstone, columns, builder, type, value);
            }
        }
    }

    private void addColumns(boolean isTombstone,
                            Columns columns,
                            ColumnBuilder builder,
                            AbstractType type,
                            ByteBuffer value) {
        if (type.isCollection()) {
            value = ByteBufferUtil.clone(value);
            CollectionType<?> collectionType = (CollectionType<?>) type;
            switch (collectionType.kind) {
                case SET: {
                    AbstractType<?> nameType = collectionType.nameComparator();
                    if (isTombstone) {
                        columns.add(builder.buildWithNull(nameType));
                    } else {
                        int colSize = CollectionSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer itemValue = CollectionSerializer.readValue(value, Server.CURRENT_VERSION);
                            addColumns(false, columns, builder, nameType, itemValue);
                        }
                    }
                    break;
                }
                case LIST: {
                    AbstractType<?> valueType = collectionType.valueComparator();
                    if (isTombstone) {
                        columns.add(builder.buildWithNull(valueType));
                    } else {
                        int colSize = ListSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer itemValue = CollectionSerializer.readValue(value, Server.CURRENT_VERSION);
                            addColumns(false, columns, builder, valueType, itemValue);
                        }
                    }
                    break;
                }
                case MAP: {
                    AbstractType<?> keyType = collectionType.nameComparator();
                    AbstractType<?> valueType = collectionType.valueComparator();
                    if (isTombstone) {
                        columns.add(builder.buildWithNull(valueType));
                    } else {
                        int colSize = MapSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer mapKey = MapSerializer.readValue(value, Server.CURRENT_VERSION);
                            ByteBuffer mapValue = MapSerializer.readValue(value, Server.CURRENT_VERSION);
                            String itemName = keyType.compose(mapKey).toString();
                            collectionType.nameComparator();
                            addColumns(false, columns, builder.withMapName(itemName), valueType, mapValue);
                        }
                    }
                    break;
                }
                default: {
                    throw new IndexException("Unknown collection type %s", collectionType.kind);
                }
            }
        } else if (type instanceof UserType) {
            UserType userType = (UserType) type;
            ByteBuffer[] values = userType.split(value);
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                String itemName = userType.fieldNameAsString(i);
                AbstractType<?> itemType = userType.fieldType(i);
                if (isTombstone) {
                    columns.add(builder.withUDTName(itemName).buildWithNull(itemType));
                } else if (values[i] != null) { // This only occurs in UDT not fully composed
                    addColumns(false, columns, builder.withUDTName(itemName), itemType, values[i]);
                }
            }
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            ByteBuffer[] values = tupleType.split(value);
            for (Integer i = 0; i < tupleType.size(); i++) {
                String itemName = i.toString();
                AbstractType<?> itemType = tupleType.type(i);
                if (isTombstone) {
                    columns.add(builder.withUDTName(itemName).buildWithNull(itemType));
                } else {
                    addColumns(false, columns, builder.withUDTName(itemName), itemType, values[i]);
                }
            }
        } else {
            if (value != null) {
                columns.add(builder.buildWithDecomposed(value, type));
            }
        }
    }

}
