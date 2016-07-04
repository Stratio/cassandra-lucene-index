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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnTest {

    @Test
    public void testCreateWithoutSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my_column").build(value);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testCreateWithMapSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my").withMapName("column").build(value);
        assertEquals("Column full name is wrong", "my$column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my", column.getMapperName());
        assertEquals("Column field name is wrong", "field$column", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testCreateWithUDTSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my").withUDTName("column").build(value);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testCreateWithComplexSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my")
                                    .withUDTName("1")
                                    .withUDTName("2")
                                    .withUDTName("3")
                                    .withMapName("4")
                                    .withMapName("5")
                                    .withMapName("6")
                                    .build(value);
        assertEquals("Column full name is wrong", "my.1.2.3$4$5$6", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.1.2.3", column.getMapperName());
        assertEquals("Column field name is wrong", "field$4$5$6", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testCreateFromComposedWithoutSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my_column").build(value);
        assertEquals("Column full name is wrong", "my_column", column.getFullName());
        assertEquals("Column cell name is wrong", "my_column", column.getCellName());
        assertEquals("Column mapper name is wrong", "my_column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testCreateFromComposedWithUDTSuffix() {
        Long value = 5L;
        Column<Long> column = Column.builder("my").withUDTName("column").build(value);
        assertEquals("Column full name is wrong", "my.column", column.getFullName());
        assertEquals("Column cell name is wrong", "my", column.getCellName());
        assertEquals("Column mapper name is wrong", "my.column", column.getMapperName());
        assertEquals("Column field name is wrong", "field", column.getFieldName("field"));
        assertEquals("Column value is wrong", value, column.getValue());
    }

    @Test
    public void testToStringWithoutSuffix() {
        Column<Long> column = Column.builder("my_column", 10).build(5L);
        assertEquals("Method #toString is wrong",
                     "Column{cell=my_column, name=my_column, value=5, deletionTime=10}",
                     column.toString());
    }

    @Test
    public void testToStringWithSuffixes() {
        Column<Long> column = Column.builder("my", 10).withUDTName("1").withMapName("2").build(5L);
        assertEquals("Method #toString is wrong",
                     "Column{cell=my, name=my.1$2, value=5, deletionTime=10}",
                     column.toString());
    }
}
