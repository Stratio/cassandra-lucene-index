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
    public void testCreateFromDecomposedWithoutSufix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Column name is wrong", name, column.getMapperName());
        assertEquals("Column fullName is wrong", name, column.getFieldName());
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromDecomposedWithSufix() {
        String name = "my";
        String sufix = "column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name+"$"+sufix, decomposedValue, type, true);
        assertEquals("Column name is wrong", name, column.getMapperName());
        assertEquals("Column fullName is wrong", "my$column", column.getFieldName());
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithoutSufix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        assertEquals("Column name is wrong", name, column.getMapperName());
        assertEquals("Column fullName is wrong", name, column.getFieldName());
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCreateFromComposedWithSufix() {
        String name = "my.column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        assertEquals("Column name is wrong", name, column.getMapperName());
        assertEquals("Column fullName is wrong", "my.column", column.getFieldName());
        assertEquals("Column type is wrong", type, column.getType());
        assertEquals("Column composedValue is wrong", composedValue, column.getComposedValue());
        assertEquals("Column decomposedValue is wrong", decomposedValue, column.getDecomposedValue());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithoutSufix() {
        String name = "my_column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        assertEquals("Method #toString is wrong",
                     "Column{fieldName=my_column, composedValue=5, type=LongType}",
                     column.toString());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testToStringFromDecomposedWithSufix() {
        String name = "my";
        String sufix = "column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name+"$"+sufix, decomposedValue, type, true);
        assertEquals("Method #toString is wrong",
                     "Column{fieldName=my$column, composedValue=5, type=LongType}",
                     column.toString());
        assertTrue("Column isNotFrozenCollection is wrong", column.isMultiCell());
    }

    @Test
    public void testCompareToWithNull() {
        String name = "my";
        String sufix = "column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name+"$"+sufix, decomposedValue, type, true);
        assertEquals("Column equals is wrong", 1, column.compareTo(null));

    }
}
