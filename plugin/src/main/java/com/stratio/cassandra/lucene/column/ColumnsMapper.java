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
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import com.stratio.cassandra.lucene.core.column.*;

import static org.apache.cassandra.transport.Server.CURRENT_VERSION;

/**
 * Class for several regular column mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnsMapper {

    /**
     * Returns the {@link Column}s contained in the specified {@link Row}.
     *
     * @param row a {@link Row}
     * @return the columns
     */
    public Columns columns(Row row) {
        Columns columns = Columns.build();
        for (ColumnDefinition columnDefinition : row.columns()) {
            if (columnDefinition.isComplex()) {
                columns = columns.add(columns(row.getComplexColumnData(columnDefinition)));
            } else {
                columns = columns.add(columns(row.getCell(columnDefinition)));
            }
        }
        return columns;
    }

    private Columns columns(ComplexColumnData complexColumnData) {
        Columns columns = Columns.build();
        if (complexColumnData != null) {
            for (Cell cell : complexColumnData) {
                columns = columns.add(columns(cell));
            }
        }
        return columns;
    }

    private Columns columns(Cell cell) {
        Columns columns = Columns.build();
        if (cell != null) {
            boolean isTombstone = cell.isTombstone();
            ColumnDefinition columnDefinition = cell.column();
            String name = columnDefinition.name.toString();
            AbstractType<?> type = cell.column().type;
            ByteBuffer value = cell.value();
            ColumnAdder adder = columns.adder(name, cell.localDeletionTime());
            if (type.isCollection() && !type.isFrozenCollection()) {
                CollectionType<?> collectionType = (CollectionType<?>) type;
                switch (collectionType.kind) {
                    case SET: {
                        type = collectionType.nameComparator();
                        value = cell.path().get(0);
                        columns = columns.add(columns(isTombstone, adder, type, value));
                        break;
                    }
                    case LIST: {
                        type = collectionType.valueComparator();
                        columns = columns.add(columns(isTombstone, adder, type, value));
                        break;
                    }
                    case MAP: {
                        type = collectionType.valueComparator();
                        ByteBuffer keyValue = cell.path().get(0);
                        AbstractType<?> keyType = collectionType.nameComparator();
                        String nameSuffix = keyType.compose(keyValue).toString();
                        columns = columns.add(columns(isTombstone, adder.withMapName(nameSuffix), type, value));
                        break;
                    }
                    default: {
                        throw new IndexException("Unknown collection type {}", collectionType.kind);
                    }
                }
            } else {
                columns = columns.add(columns(isTombstone, adder, type, value));
            }
        }
        return columns;
    }

    private Columns columns(boolean isTombstone,
                            ColumnAdder adder,
                            AbstractType type,
                            ByteBuffer value) {
        Columns columns = Columns.build();
        if (type.isCollection()) {
            value = ByteBufferUtil.clone(value);
            CollectionType<?> collectionType = (CollectionType<?>) type;
            switch (collectionType.kind) {
                case SET: {
                    AbstractType<?> nameType = collectionType.nameComparator();
                    if (isTombstone) {
                        columns = adder.add();
                    } else {
                        int colSize = CollectionSerializer.readCollectionSize(value, CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer itemValue = CollectionSerializer.readValue(value, CURRENT_VERSION);
                            columns = columns.add(columns(false, adder, nameType, itemValue));
                        }
                    }
                    break;
                }
                case LIST: {
                    AbstractType<?> valueType = collectionType.valueComparator();
                    if (isTombstone) {
                        columns = adder.add();
                    } else {
                        int colSize = ListSerializer.readCollectionSize(value, CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer itemValue = CollectionSerializer.readValue(value, CURRENT_VERSION);
                            columns = columns.add(columns(false, adder, valueType, itemValue));
                        }
                    }
                    break;
                }
                case MAP: {
                    AbstractType<?> keyType = collectionType.nameComparator();
                    AbstractType<?> valueType = collectionType.valueComparator();
                    if (isTombstone) {
                        columns = adder.add();
                    } else {
                        int colSize = MapSerializer.readCollectionSize(value, CURRENT_VERSION);
                        for (int j = 0; j < colSize; j++) {
                            ByteBuffer mapKey = MapSerializer.readValue(value, CURRENT_VERSION);
                            ByteBuffer mapValue = MapSerializer.readValue(value, CURRENT_VERSION);
                            String itemName = keyType.compose(mapKey).toString();
                            collectionType.nameComparator();
                            columns = columns.add(columns(false, adder.withMapName(itemName), valueType, mapValue));
                        }
                    }
                    break;
                }
                default: {
                    throw new IndexException("Unknown collection type {}", collectionType.kind);
                }
            }
        } else if (type instanceof UserType) {
            UserType userType = (UserType) type;
            ByteBuffer[] values = userType.split(value);
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                String itemName = userType.fieldNameAsString(i);
                AbstractType<?> itemType = userType.fieldType(i);
                if (isTombstone) {
                    columns = adder.withUDTName(itemName).add();
                } else if (values[i] != null) { // This only occurs in UDT not fully composed
                    columns = columns.add(columns(false, adder.withUDTName(itemName), itemType, values[i]));
                }
            }
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            ByteBuffer[] values = tupleType.split(value);
            for (Integer i = 0; i < tupleType.size(); i++) {
                String itemName = i.toString();
                AbstractType<?> itemType = tupleType.type(i);
                if (isTombstone) {
                    columns = adder.withUDTName(itemName).add();
                } else {
                    columns = columns.add(columns(false, adder.withUDTName(itemName), itemType, values[i]));
                }
            }
        } else {
            if (value != null) {
                columns = adder.add(value(value, type));
            }
        }
        return columns;
    }

    public static <T> Column<?> column(String name, ByteBuffer bb, AbstractType<T> type) {
        return Column.build(name, value(bb, type));
    }

    public static Object value(ByteBuffer bb, AbstractType<?> type) {
        if (type instanceof SimpleDateType) {
            Locale locale = Locale.getDefault(Locale.Category.FORMAT);
            Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), locale);
            long timestamp = SimpleDateType.instance.toTimeInMillis(bb);
            timestamp -= calendar.getTimeZone().getOffset(timestamp);
            return new Date(timestamp);
        }
        return type.compose(bb);
    }

}
