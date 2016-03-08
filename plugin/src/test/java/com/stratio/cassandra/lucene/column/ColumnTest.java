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

package com.stratio.cassandra.lucene.column;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnTest {

    @Test
    public void testCreateFromDecomposedWithoutSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testCreateFromDecomposedWithMapSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").withMapName("column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my$column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my", column.getMapperName());
        assertEquals("Column field name is wrong", "field$column", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testCreateFromDecomposedWithUDTSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").withUDTName("column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testCreateFromDecomposedWithComplexSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my")
                                    .withUDTName("1")
                                    .withUDTName("2")
                                    .withUDTName("3")
                                    .withMapName("4")
                                    .withMapName("5")
                                    .withMapName("6")
                                    .buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.1.2.3$4$5$6", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.1.2.3", column.getMapperName());
        assertEquals("Column field name is wrong", "field$4$5$6", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testCreateFromComposedWithoutSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testCreateFromComposedWithUDTSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").withUDTName("column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column buildWithComposed is wrong", composedValue, column.getComposedValue());
        assertEquals("Column buildWithDecomposed is wrong", decomposedValue, column.getDecomposedValue());
    }

    @Test
    public void testToStringFromDecomposedWithoutSuffix() {
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").buildWithDecomposed(decomposedValue, type);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my_column, buildWithComposed=5, type=LongType}",
                     column.toString());
    }

    @Test
    public void testToStringFromDecomposedWithSuffixes() {
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my")
                                    .withUDTName("1")
                                    .withMapName("2")
                                    .buildWithDecomposed(decomposedValue, type);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my.1$2, buildWithComposed=5, type=LongType}",
                     column.toString());
    }
}
