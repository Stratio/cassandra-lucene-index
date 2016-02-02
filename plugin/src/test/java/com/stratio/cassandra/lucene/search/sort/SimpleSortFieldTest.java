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
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
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
public class SimpleSortFieldTest {

    @Test
    public void testBuild() {
        SimpleSortField sortField = new SimpleSortField("field", true);
        assertEquals("SortField is not created", "field", sortField.getField());
        assertTrue("SortField reverse is not set", sortField.isReverse());
    }

    @Test
    public void testBuildDefaults() {
        SimpleSortField sortField = new SimpleSortField("field", null);
        assertEquals("SortField is not created", "field", sortField.getField());
        assertEquals("SortField reverse is not set to default", SortField.DEFAULT_REVERSE, sortField.isReverse());
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new SimpleSortField(null, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildNBlankField() {
        new SimpleSortField(" ", null);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithoutField() {
        new SimpleSortField(null, null);
    }

    @Test
    public void testSortFieldDefaults() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", null);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertEquals("SortField reverse is wrong", SortField.DEFAULT_REVERSE, luceneSortField.getReverse());
    }

    @Test
    public void testSimpleSortField() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", false);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldReverse() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", true);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField name is wrong", "field", luceneSortField.getField());
        assertTrue("sortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test(expected = IndexException.class)
    public void testSortFieldUnsorted() {
        Schema schema = schema().mapper("field", stringMapper().sorted(false)).build();
        SimpleSortField sortField = new SimpleSortField("field", false);
        sortField.sortField(schema);
    }

    @Test
    public void testSortFieldScoreDefaults() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("score", null);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertEquals("SortField reverse is wrong", SortField.DEFAULT_REVERSE, luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldScore() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("score", false);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test
    public void testSortFieldScoreReverse() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("score", true);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField);
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test(expected = IndexException.class)
    public void testSortFieldWithoutMapper() {
        Schema schema = schema().build();
        SimpleSortField sortField = new SimpleSortField("field", true);
        sortField.sortField(schema);
    }

    @Test
    public void testComparatorNatural() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", false);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<String> lowerColumn = Column.builder("field").buildWithComposed("a", UTF8Type.instance);
        Column<String> upperColumn = Column.builder("field").buildWithComposed("z", UTF8Type.instance);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals("SortField columns comparator is wrong", -25, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorReverse() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", true);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<String> lowerColumn = Column.builder("field").buildWithComposed("a", UTF8Type.instance);
        Column<String> upperColumn = Column.builder("field").buildWithComposed("z", UTF8Type.instance);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals("SortField columns comparator is wrong", 25, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorNullColumns() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", true);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<String> column = Column.builder("field").buildWithComposed("a", UTF8Type.instance);
        Columns columns = new Columns().add(column);

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns, null));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(null, columns));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(null, null));
    }

    @Test
    public void testComparatorNullColumn() {

        Schema schema = schema().mapper("field", stringMapper().sorted(true)).build();

        SimpleSortField sortField = new SimpleSortField("field", true);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Columns columns1 = new Columns().add(Column.builder("field").buildWithComposed("a", UTF8Type.instance));
        Columns columns2 = new Columns();

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(columns2, columns1));
    }

    @Test
    public void testCompareColumns() {

        SimpleSortField sortField = new SimpleSortField("field", true);

        Column column1 = Column.builder("field").buildWithComposed("a", UTF8Type.instance);
        Column column2 = Column.builder("field").buildWithComposed("z", UTF8Type.instance);
        SingleColumnMapper mapper = stringMapper().build("field");
        Columns columns1 = new Columns(column1);
        Columns columns2 = new Columns(column2);
        Columns emptyColumns = new Columns();

        assertEquals("SortField compare is wrong", 25, sortField.compare(mapper, columns1, columns2));
        assertEquals("SortField compare is wrong", -25, sortField.compare(mapper, columns2, columns1));
        assertEquals("SortField compare is wrong", 1, sortField.compare(mapper, emptyColumns, columns1));
        assertEquals("SortField compare is wrong", -1, sortField.compare(mapper, columns2, emptyColumns));
        assertEquals("SortField compare is wrong", 0, sortField.compare(mapper, emptyColumns, emptyColumns));
    }

    @Test
    public void testCompareColumn() {

        SimpleSortField sortField = new SimpleSortField("field", true);

        Comparable column1 = "a";
        Comparable column2 = "z";

        assertEquals("SortField compare is wrong", 25, sortField.compare(column1, column2));
        assertEquals("SortField compare is wrong", -25, sortField.compare(column2, column1));
        assertEquals("SortField compare is wrong", 1, sortField.compare(null, column1));
        assertEquals("SortField compare is wrong", -1, sortField.compare(column2, null));
        assertEquals("SortField compare is wrong", 0, sortField.compare(null, null));
    }

    private static SortField nullSortField() {
        return null;
    }

    private static Object nullInt() {
        return null;
    }

    @Test
    public void testEquals() {
        SimpleSortField sortField = new SimpleSortField("field", true);
        assertEquals("SortField equals is wrong", sortField, sortField);
        assertEquals("SortField equals is wrong", sortField, new SimpleSortField("field", true));
        assertFalse("SortField equals is wrong", sortField.equals(new SimpleSortField("field2", true)));
        assertFalse("SortField equals is wrong", sortField.equals(new SimpleSortField("field", false)));
        assertFalse("SortField equals is wrong", sortField.equals(nullInt()));
        assertFalse("SortField equals is wrong", sortField.equals(nullSortField()));
    }

    @Test
    public void testEqualsWithNull() {
        SimpleSortField sortField = new SimpleSortField("field", true);
        assertFalse("SortField equals is wrong", sortField.equals(null));
        assertFalse("SortField equals is wrong", sortField.equals(new Integer(0)));
    }

    @Test
    public void testHashCode() {
        assertEquals("SortField equals is wrong", -1274708409, new SimpleSortField("field", true).hashCode());
        assertEquals("SortField equals is wrong", -1274708410, new SimpleSortField("field", false).hashCode());
    }

    @Test
    public void testToString() {
        SimpleSortField sortField = new SimpleSortField("field", null);
        assertEquals("Method #toString is wrong", "SimpleSortField{field=field, reverse=false}", sortField.toString());
    }
}