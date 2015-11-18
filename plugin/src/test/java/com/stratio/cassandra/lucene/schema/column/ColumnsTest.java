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

import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ColumnsTest {

    @Test
    public void testBuild() {
        Columns columns = new Columns();
        assertEquals(0, columns.size());
    }

    @Test
    public void testAdd() {
        Columns columns = new Columns();
        columns.add(Column.fromComposed("field1", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field2", "value2", UTF8Type.instance, false));
        assertEquals("Columns size is wrong", 2, columns.size());
    }

    @Test
    public void testAddAll() {
        Columns columns = new Columns();
        columns.add(new Columns().add(Column.fromComposed("field1", "value1", UTF8Type.instance, false))
                                 .add(Column.fromComposed("field2", "value2", UTF8Type.instance, false)));
        assertEquals("Columns size is wrong", 2, columns.size());
    }

    @Test
    public void testIterator() {
        Columns columns = new Columns();
        columns.add(new Columns().add(Column.fromComposed("field1", "value1", UTF8Type.instance, false))
                                 .add(Column.fromComposed("field2", "value2", UTF8Type.instance, false)));
        List<Column<?>> columnList = new ArrayList<>();
        for (Column<?> column : columns) {
            columnList.add(column);
        }
        assertEquals("Columns size is wrong", 2, columnList.size());
    }

    @Test
    public void testGetColumnsByFullName() {
        Columns columns = new Columns();
        columns.add(Column.fromComposed("field1", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field2", "value2", UTF8Type.instance, false));
        assertEquals("Columns size is wrong", 1, columns.getColumnsByFullName("field1").size());
        assertEquals("Columns size is wrong", 1, columns.getColumnsByFullName("field2").size());
        assertTrue("Columns getColumnsByFullName is wrong", columns.getColumnsByFullName("field3").isEmpty());
    }

    @Test
    public void testGetColumnsByName() {
        Columns columns = new Columns();
        columns.add(Column.fromComposed("field1$item1", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field1$item2", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field2$item1", "value2", UTF8Type.instance, false));
        assertEquals("Columns size is wrong", 2, columns.getColumnsByName("field1").size());
        assertEquals("Columns size is wrong", 0, columns.getColumnsByFullName("field1").size());
        assertEquals("Columns size is wrong", 1, columns.getColumnsByFullName("field1$item1").size());
        assertEquals("Columns size is wrong", 1, columns.getColumnsByFullName("field1$item2").size());
        assertEquals("Columns size is wrong", 1, columns.getColumnsByName("field2").size());
        assertEquals("Columns size is wrong", 0, columns.getColumnsByFullName("field2").size());
        assertEquals("Columns size is wrong",
                     1,
                     columns.getColumnsByFullName("field2$item1").size());
                                                  assertTrue("Columns getColumnsByFullName is wrong",
                                                             columns.getColumnsByFullName("field3").isEmpty());
    }

    @Test
    public void testToString() {
        assertEquals("Method #toString is wrong", "Columns{}", new Columns().toString());
    }

    @Test
    public void testToStringWithColumns() {
        Columns columns = new Columns();
        columns.add(Column.fromComposed("field1$item1", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field1$item2", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field2$item1", "value2", UTF8Type.instance, false));
        assertEquals("Method #toString is wrong",
                     "Columns{field1$item1=value1, field1$item2=value1, field2$item1=value2}",
                     columns.toString());
    }
}
