/*
 * Copyright 2015, Stratio.
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

import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
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
        assertEquals(2, columns.size());
    }

    @Test
    public void testAddAll() {
        Columns columns = new Columns();
        columns.add(new Columns().add(Column.fromComposed("field1", "value1", UTF8Type.instance, false))
                                 .add(Column.fromComposed("field2", "value2", UTF8Type.instance, false)));
        assertEquals(2, columns.size());
    }

    @Test
    public void testIterator() {
        Columns columns = new Columns();
        columns.add(new Columns().add(Column.fromComposed("field1", "value1", UTF8Type.instance, false))
                                 .add(Column.fromComposed("field2", "value2", UTF8Type.instance, false)));
        List<Column> columnList = new ArrayList<>();
        for (Column column : columns) {
            columnList.add(column);
        }
        assertEquals(2, columnList.size());
    }

    @Test
    public void testGet() {
        Columns columns = new Columns();
        columns.add(Column.fromComposed("field1", "value1", UTF8Type.instance, false));
        columns.add(Column.fromComposed("field2", "value2", UTF8Type.instance, false));
        assertNotNull(columns.getColumn("field1"));
        assertNotNull(columns.getColumn("field2"));
        assertNull(columns.getColumn("field3"));
    }

    @Test
    public void testToString() {
        assertEquals("Columns{columns=[]}", new Columns().toString());
    }
}
