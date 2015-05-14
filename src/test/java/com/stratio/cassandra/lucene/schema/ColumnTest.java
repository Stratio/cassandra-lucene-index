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
package com.stratio.cassandra.lucene.schema;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnTest {

    @Test
    public void testCreateFromDecomposedWithoutSufix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        Assert.assertEquals(name, column.getName());
        Assert.assertEquals(name, column.getFullName());
        Assert.assertEquals(type, column.getType());
        Assert.assertEquals(composedValue, column.getComposedValue());
        Assert.assertEquals(decomposedValue, column.getDecomposedValue());
        Assert.assertTrue(column.isCollection());
    }

    @Test
    public void testCreateFromDecomposedWithSufix() {
        String name = "my";
        String sufix = "column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, sufix, decomposedValue, type, true);
        Assert.assertEquals(name, column.getName());
        Assert.assertEquals("my.column", column.getFullName());
        Assert.assertEquals(type, column.getType());
        Assert.assertEquals(composedValue, column.getComposedValue());
        Assert.assertEquals(decomposedValue, column.getDecomposedValue());
        Assert.assertTrue(column.isCollection());
    }

    @Test
    public void testCreateFromComposedWithoutSufix() {
        String name = "my_column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        Assert.assertEquals(name, column.getName());
        Assert.assertEquals(name, column.getFullName());
        Assert.assertEquals(type, column.getType());
        Assert.assertEquals(composedValue, column.getComposedValue());
        Assert.assertEquals(decomposedValue, column.getDecomposedValue());
        Assert.assertTrue(column.isCollection());
    }

    @Test
    public void testCreateFromComposedWithSufix() {
        String name = "my.column";
        AbstractType<Long> type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromComposed(name, composedValue, type, true);
        Assert.assertEquals(name, column.getName());
        Assert.assertEquals("my.column", column.getFullName());
        Assert.assertEquals(type, column.getType());
        Assert.assertEquals(composedValue, column.getComposedValue());
        Assert.assertEquals(decomposedValue, column.getDecomposedValue());
        Assert.assertTrue(column.isCollection());
    }

    @Test
    public void testToStringFromDecomposedWithoutSufix() {
        String name = "my_column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, decomposedValue, type, true);
        Assert.assertEquals("Column{fullName=my_column, composedValue=5, type=LongType}", column.toString());
        Assert.assertTrue(column.isCollection());
    }

    @Test
    public void testToStringFromDecomposedWithSufix() {
        String name = "my";
        String sufix = "column";
        LongType type = LongType.instance;
        Long composedValue = 5L;
        ByteBuffer decomposedValue = type.decompose(composedValue);
        Column<Long> column = Column.fromDecomposed(name, sufix, decomposedValue, type, true);
        Assert.assertEquals("Column{fullName=my.column, composedValue=5, type=LongType}", column.toString());
        Assert.assertTrue(column.isCollection());
    }
}
