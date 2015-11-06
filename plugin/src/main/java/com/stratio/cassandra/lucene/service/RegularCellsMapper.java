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
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
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




    private Columns processUserTypeContents(String name, ByteBuffer value, UserType userType) {
        Columns columns = new Columns();
        ByteBuffer[] values = userType.split(value);
        for (int i = 0; i < userType.fieldNames().size(); i++) {
            String fieldName = name+"."+userType.fieldNameAsString(i);
            AbstractType<?> type = userType.fieldType(i);
            if (type.isCollection()) {

                CollectionType<?> collectionType = (CollectionType<?>) type;
                switch (collectionType.kind) {
                    case SET: {
                        AbstractType<?> nameType = collectionType.nameComparator();
                        logger.debug("RCM: processUDT SET name: "+fieldName+" cell.value: "+ ByteBufferUtil.bytesToHex(values[i]));
                        int colSize= CollectionSerializer.readCollectionSize(values[i], Server.CURRENT_VERSION);
                        for (int j=0;j<colSize;j++) {
                            columns.add(Column.fromDecomposed(fieldName,CollectionSerializer.readValue(values[i], Server.CURRENT_VERSION), nameType, true));
                        }
                        break;
                    }
                    case LIST: {
                        AbstractType<?> valueType = collectionType.valueComparator();
                        logger.debug("RCM: processUDT LIST name: "+fieldName+" cell.value: "+ByteBufferUtil.bytesToHex(values[i]));
                        int colSize= CollectionSerializer.readCollectionSize(values[i], Server.CURRENT_VERSION);
                        for (int j=0;j<colSize;j++) {
                            columns.add(Column.fromDecomposed(fieldName, CollectionSerializer.readValue(values[i],
                                                                                                        Server.CURRENT_VERSION), valueType, true));
                        }
                        break;
                    }
                    case MAP: {
                        AbstractType<?> keyType = collectionType.nameComparator();
                        AbstractType<?> valueType = collectionType.valueComparator();
                        logger.debug("RCM: processUDT MAP name: "+fieldName+" cell.value: "+ByteBufferUtil.bytesToHex(values[i]));
                        int colSize= MapSerializer.readCollectionSize(values[i], Server.CURRENT_VERSION);
                        for (int j=0;j<colSize;j++) {
                            ByteBuffer mapKey=MapSerializer.readValue(values[i], Server.CURRENT_VERSION);
                            ByteBuffer mapValue=MapSerializer.readValue(values[i], Server.CURRENT_VERSION);
                            String nameSuffix = keyType.compose(mapKey).toString();
                            columns.add(Column.fromDecomposed(fieldName, nameSuffix, mapValue, valueType, true));
                        }
                        break;
                    }
                }
            } else if (type instanceof UserType) {
                UserType childType= (UserType) type;
                columns.add(processUserTypeContents(fieldName, values[i], childType));
            } else {
                columns.add(Column.fromDecomposed(fieldName, values[i], type, false));
            }
        }
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
                    logger.debug("RCM: process SET "+name+" cell.value: "+ByteBufferUtil.bytesToHex(cell.value())+ " cell.name.collectionElement:"+ByteBufferUtil.bytesToHex(
                            value));

                    columns.add(Column.fromDecomposed(name, value, type, true));
                    break;
                }
                case LIST: {
                    AbstractType<?> type = collectionType.valueComparator();
                    logger.debug("RCM: process LIST "+name+" value: "+ByteBufferUtil.bytesToHex(cell.value()));
                    columns.add(Column.fromDecomposed(name, cell.value(), type, true));
                    break;
                }
                case MAP: {
                    AbstractType<?> type = collectionType.valueComparator();
                    ByteBuffer keyValue = cell.name().collectionElement();
                    AbstractType<?> keyType = collectionType.nameComparator();
                    String nameSuffix = keyType.compose(keyValue).toString();
                    logger.debug("RCM: process MAP "+name+" value: "+ByteBufferUtil.bytesToHex(cell.value()));
                    columns.add(Column.fromDecomposed(name, nameSuffix, cell.value(), type, true));
                    break;
                }
            }
        } else if (valueType instanceof UserType) {
            UserType userType = (UserType) valueType;

            columns.add(processUserTypeContents(cell.name().cql3ColumnName(metadata).toString(), cell.value(), userType));
        } else {
            columns.add(Column.fromDecomposed(name, cell.value(), valueType, false));
        }

        return columns;
    }

    private Columns process1(String udtNames, String mapKeysSuffix, AbstractType type,ByteBuffer value,boolean hasAnyNotFrozenCollectionAsParent) {

        Columns columns= new Columns();
        logger.debug("RCM: process1 processing udtNames: "+udtNames+" mapKeysSuffix: "+mapKeysSuffix+ " value: "+ ByteBufferUtils.toHex(value)+" hasAnyNotFrozenCollectionAsParent: "+(hasAnyNotFrozenCollectionAsParent?"true":"false"));
        if (type.isCollection()) {
            CollectionType<?> collectionType = (CollectionType<?>) type;
            switch (collectionType.kind) {
                case SET: {
                    AbstractType<?> nameType = collectionType.nameComparator();
                    int colSize= CollectionSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                    for (int j=0;j<colSize;j++) {
                        ByteBuffer itemValue=CollectionSerializer.readValue(value, Server.CURRENT_VERSION);
                        columns.add(process1(udtNames,mapKeysSuffix,nameType,itemValue,hasAnyNotFrozenCollectionAsParent));
                    }
                    break;
                }
                case LIST: {
                    AbstractType<?> valueType = collectionType.valueComparator();
                    int colSize= CollectionSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                    for (int j=0;j<colSize;j++) {
                        ByteBuffer itemValue=CollectionSerializer.readValue(value, Server.CURRENT_VERSION);
                        columns.add(process1(udtNames,mapKeysSuffix, valueType,itemValue,hasAnyNotFrozenCollectionAsParent));
                    }
                    break;
                }
                case MAP: {
                    AbstractType<?> keyType = collectionType.nameComparator();
                    AbstractType<?> valueType = collectionType.valueComparator();
                    //logger.debug("RCM: processUDT MAP name: "+fieldName+" cell.value: "+ByteBufferUtil.bytesToHex(value));
                    int colSize= MapSerializer.readCollectionSize(value, Server.CURRENT_VERSION);
                    for (int j=0;j<colSize;j++) {
                        ByteBuffer mapKey=MapSerializer.readValue(value, Server.CURRENT_VERSION);
                        ByteBuffer mapValue=MapSerializer.readValue(value, Server.CURRENT_VERSION);

                        String itemName=keyType.compose(mapKey).toString();
                        columns.add(process1(udtNames,join(mapKeysSuffix,itemName), valueType,mapValue,hasAnyNotFrozenCollectionAsParent));

                        //columns.add(Column.fromDecomposed(fieldName, nameSuffix, mapValue, valueType, true));
                    }
                    break;
                }
            }

        } else if (type instanceof UserType) {
            UserType userType=(UserType)type;
            ByteBuffer[] values = userType.split(value);
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                String itemName=userType.fieldNameAsString(i);

                AbstractType<?> itemType = userType.fieldType(i);
                columns.add(process1(join(udtNames,itemName),mapKeysSuffix, itemType,values[i],hasAnyNotFrozenCollectionAsParent));
            }
        } else {//basic type
            columns.add(Column.fromDecomposed(udtNames,mapKeysSuffix,value,type, hasAnyNotFrozenCollectionAsParent));
        }
        return columns;

    }
    private String join(String left, String right) {
        return (left==null)?right:left+"."+right;
    }

    /**
     * Returns the columns contained in the regular cells specified row. Note that not all the contained columns are
     * returned, but only the regular cell ones.
     *
     * @param columnFamily A row column family.
     * @return The columns contained in the regular cells specified row.
     */
    public Columns columns(ColumnFamily columnFamily) {

        logger.debug("RCM: columns ");
        Columns columns = new Columns();
        // Stuff for grouping collection columns (sets, lists and maps)
        String name;
        for (Cell cell : columnFamily) {

            CellName cellName = cell.name();
            name = cellName.cql3ColumnName(metadata).toString();
            if (name.length()==0) continue;

            if (!schema.maps(name)) {
                continue;
            }

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(cellName);
            if (columnDefinition == null) {
                continue;
            }

            AbstractType<?> valueType = columnDefinition.type;

            if ((valueType.isCollection()) && (!valueType.isFrozenCollection())) {
                logger.debug("RCM: "+name+" isNotFrozenCollection");
                CollectionType<?> collectionType = (CollectionType<?>) valueType;
                switch (collectionType.kind) {
                    case SET: {
                        AbstractType<?> type = collectionType.nameComparator();
                        ByteBuffer value = cell.name().collectionElement();
                        logger.debug("RCM: process SET "+name+" cell.value: "+ByteBufferUtil.bytesToHex(cell.value())+ " cell.name.collectionElement:"+ByteBufferUtil.bytesToHex(
                                value));
                        columns.add(process1(name,null, type, value,true));
                        break;
                    }
                    case LIST: {
                        AbstractType<?> type = collectionType.valueComparator();
                        logger.debug("RCM: process LIST "+name+" value: "+ByteBufferUtil.bytesToHex(cell.value()));
                        columns.add(process1(name,null, type, cell.value(),true));
                        break;
                    }
                    case MAP: {
                        AbstractType<?> type = collectionType.valueComparator();
                        ByteBuffer keyValue = cell.name().collectionElement();
                        AbstractType<?> keyType = collectionType.nameComparator();
                        String nameSuffix = keyType.compose(keyValue).toString();
                        logger.debug("RCM: process MAP "+name+" value: "+ByteBufferUtil.bytesToHex(cell.value()));

                        columns.add(process1(name,nameSuffix,type, cell.value(),true));
                        break;
                    }
                }
            } else {
                columns.add(process1(name, null, valueType, cell.value(), false));
            }
        }
        logger.debug("RCM columns returning: "+columns.toString());
        return columns;
    }
}

