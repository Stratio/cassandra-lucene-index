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

package com.stratio.cassandra.lucene.schema.column;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnTest {

    @Test
    public void testCreateFromDecomposedWithoutSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").multiCell(true).decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithMapSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").mapName("column").decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my$column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my", column.getMapperName());
        assertEquals("Column field name is wrong", "field$column", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithUDTSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").udtName("column").decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithComplexSuffix() {
        String name = "my.1.2.3$4$5$6";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my")
                                    .udtName("1")
                                    .udtName("2")
                                    .udtName("3")
                                    .mapName("4")
                                    .mapName("5")
                                    .mapName("6")
                                    .decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.1.2.3$4$5$6", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.1.2.3", column.getMapperName());
        assertEquals("Column field name is wrong", "field$4$5$6", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithoutSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithUDTSuffix() {
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").udtName("column").decomposedValue(decomposedValue, type);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithoutSuffix() {
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my_column").decomposedValue(decomposedValue, type);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my_column, composedValue=5, type=LongType}",
                     column.toString());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithSuffixes() {
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.builder("my").udtName("1").mapName("2").decomposedValue(decomposedValue, type);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my.1$2, composedValue=5, type=LongType}",
                     column.toString());
        assertFalse("Column multiCell is wrong", column.isMultiCell());
    }
}
