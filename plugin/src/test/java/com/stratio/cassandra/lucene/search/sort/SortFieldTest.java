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

package com.stratio.cassandra.lucene.search.sort;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.Test;

import java.util.Comparator;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.apache.lucene.search.SortField.FIELD_SCORE;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortFieldTest {

    @Test
    public void testBuild() {
        SortField sortField = new SortField("field", true);
        assertEquals("SortField is not created", "field", sortField.getField());
        assertTrue("SortField reverse is not set", sortField.isReverse());
    }

    @Test
    public void testBuildDefaults() {
        SortField sortField = new SortField("field", null);
        assertEquals("SortField is not created", "field", sortField.getField());
        assertEquals("SortField reverse is not set to default", SortField.DEFAULT_REVERSE, sortField.isReverse());
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new SortField(null, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildNBlankField() {
        new SortField(" ", null);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithoutField() {
        new SortField(null, null);
    }

    @Test
    public void testSortFieldDefaults() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("field", null);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertEquals("SortField reverse is wrong", SortField.DEFAULT_REVERSE, luceneSortField.getReverse());
    }

    @Test
    public void testSortField() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("field", false);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldReverse() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("field", true);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertTrue("sortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test(expected = IndexException.class)
    public void testSortFieldUnsorted() {
        Schema schema = schema().mapper("field", stringMapper().sorted(false)).build();
        SortField sortField = new SortField("field", false);
        sortField.sortField(schema);
    }

    @Test
    public void testSortFieldScoreDefaults() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("score", null);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertEquals("SortField reverse is wrong", SortField.DEFAULT_REVERSE, luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldScore() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("score", false);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldScoreReverse() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SortField sortField = new SortField("score", true);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test(expected = IndexException.class)
    public void testSortFieldWithoutMapper() {
        Schema schema = schema().build();
        SortField sortField = new SortField("field", true);
        sortField.sortField(schema);
    }

    @Test
    public void testComparatorNatural() {

        SortField sortField = new SortField("field", false);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> lowerColumn = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Column<String> upperColumn = Column.fromComposed("field", "z", UTF8Type.instance, false);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals("SortField columns comparator is wrong", -25, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorReverse() {

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> lowerColumn = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Column<String> upperColumn = Column.fromComposed("field", "z", UTF8Type.instance, false);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals("SortField columns comparator is wrong", 25, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorNullColumns() {

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> column = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Columns columns = new Columns().add(column);

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns, null));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(null, columns));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(null, null));
    }

    @Test
    public void testComparatorNullColumn() {

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Columns columns1 = new Columns().add(Column.fromComposed("field", "a", UTF8Type.instance, false));
        Columns columns2 = new Columns();

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(columns2, columns1));
    }

    @Test
    public void testCompareColumn() {

        SortField sortField = new SortField("field", true);

        Column column1 = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Column column2 = Column.fromComposed("field", "z", UTF8Type.instance, false);

        assertEquals("SortField compare is wrong", 25, sortField.compare(column1, column2));
        assertEquals("SortField compare is wrong", -25, sortField.compare(column2, column1));
        assertEquals("SortField compare is wrong", 1, sortField.compare(nullColumn(), column1));
        assertEquals("SortField compare is wrong", -1, sortField.compare(column2, nullColumn()));
        assertEquals("SortField compare is wrong", 0, sortField.compare(nullColumn(), nullColumn()));
    }

    private static Column nullColumn() {
        return null;
    }

    private static SortField nullSortField() {
        return null;
    }

    private static Object nullInt() {
        return null;
    }

    @Test
    public void testEquals() {
        SortField sortField = new SortField("field", true);
        assertEquals("SortField equals is wrong", sortField, sortField);
        assertEquals("SortField equals is wrong", sortField, new SortField("field", true));
        assertFalse("SortField equals is wrong", sortField.equals(new SortField("field2", true)));
        assertFalse("SortField equals is wrong", sortField.equals(new SortField("field", false)));
        assertFalse("SortField equals is wrong", sortField.equals(nullInt()));
        assertFalse("SortField equals is wrong", sortField.equals(nullSortField()));
    }

    @Test
    public void testEqualsWithNull() {
        SortField sortField = new SortField("field", true);
        assertFalse("SortField equals is wrong", sortField.equals(null));
        assertFalse("SortField equals is wrong", sortField.equals(new Integer(0)));
    }

    @Test
    public void testHashCode() {
        assertEquals("SortField equals is wrong", -1274708409, new SortField("field", true).hashCode());
        assertEquals("SortField equals is wrong", -1274708410, new SortField("field", false).hashCode());
    }

    @Test
    public void testToString() {
        SortField sortField = new SortField("field", null);
        assertEquals("Method #toString is wrong", "SortField{field=field, reverse=false}", sortField.toString());
    }
}
