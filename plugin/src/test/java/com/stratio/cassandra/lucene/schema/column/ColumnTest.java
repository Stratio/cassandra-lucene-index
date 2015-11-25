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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnTest {

    @Test
    public void testCreateFromDecomposedWithoutSuffix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", name, column.getCellName());
        assertEquals("Column mapper name is wrong", name, column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithMapSuffix() {
        String name = "my$column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my", column.getMapperName());
        assertEquals("Column field name is wrong", "field$column", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithUDTSuffix() {
        String name = "my.column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithComplexSuffix() {
        String name = "my.1$2$3.4.5$6.7";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.1.4.5.7", column.getMapperName());
        assertEquals("Column field name is wrong", "field$2$3$6", column.getFieldName("field"));
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithoutSuffix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", name, column.getCellName());
        assertEquals("Column mapper name is wrong", name, column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithUDTSuffix() {
        String name = "my.column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        assertEquals("Column full name is wrong", name, column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", name, column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithoutSuffix() {
        String name = "my_column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my_column, composedValue=5, type=LongType}",
                     column.toString());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithSuffixes() {
        String name = "my$column.name";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Method #toString is wrong",
                     "Column{fullName=my$column.name, composedValue=5, type=LongType}",
                     column.toString());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testGetMapperName() {
        Column<Long> column = Column.fromComposed("address.persons$john$doe.name$one", 2L, LongType.instance, true);
        assertEquals("GetMapperName is wrong", "address.persons.name", column.getMapperName());
    }
}
