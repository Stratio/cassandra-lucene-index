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
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortFieldTest {

    @Test
    public void testBuild() {
        SortField sortField = new SortField("field", true);
        assertEquals("field", sortField.getField());
        assertTrue(sortField.isReverse());
    }

    @Test
    public void testBuildDefaults() {
        SortField sortField = new SortField("field", null);
        assertEquals("field", sortField.getField());
        assertEquals(SortField.DEFAULT_REVERSE, sortField.isReverse());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullField() {
        new SortField(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNBlankField() {
        new SortField(" ", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutField() {
        new SortField(null, null);
    }

    @Test
    public void testSortField() {

        ColumnMapper mapper = new ColumnMapperString("field", null, null, null);

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.DEFAULT.get());
        when(schema.getMapper("field")).thenReturn(mapper);

        SortField sortField = new SortField("field", true);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull(luceneSortField);
        assertEquals(org.apache.lucene.search.SortField.class, luceneSortField.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSortFieldWithoutMapper() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.DEFAULT.get());

        SortField sortField = new SortField("field", true);
        sortField.sortField(schema);
    }

    @Test
    public void testComparatorNatural() {

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.DEFAULT.get());
        when(schema.getMapper("field")).thenReturn(new ColumnMapperString("field", null, null, null));

        SortField sortField = new SortField("field", false);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> lowerColumn = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Column<String> upperColumn = Column.fromComposed("field", "z", UTF8Type.instance, false);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals(-25, comparator.compare(columns1, columns2));
        assertEquals(0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorReverse() {

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.DEFAULT.get());
        when(schema.getMapper("field")).thenReturn(new ColumnMapperString("field", null, null, null));

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> lowerColumn = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Column<String> upperColumn = Column.fromComposed("field", "z", UTF8Type.instance, false);
        Columns columns1 = new Columns().add(lowerColumn);
        Columns columns2 = new Columns().add(upperColumn);

        assertEquals(25, comparator.compare(columns1, columns2));
        assertEquals(0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorNullColumns() {

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Column<String> column = Column.fromComposed("field", "a", UTF8Type.instance, false);
        Columns columns = new Columns().add(column);

        assertEquals(-1, comparator.compare(columns, null));
        assertEquals(1, comparator.compare(null, columns));
        assertEquals(0, comparator.compare(null, null));
    }

    @Test
    public void testComparatorNullColumn() {

        SortField sortField = new SortField("field", true);
        Comparator<Columns> comparator = sortField.comparator();

        Columns columns1 = new Columns().add(Column.fromComposed("field", "a", UTF8Type.instance, false));
        Columns columns2 = new Columns();

        assertEquals(-1, comparator.compare(columns1, columns2));
        assertEquals(1, comparator.compare(columns2, columns1));
    }

    @Test
    public void testEquals() {
        assertFalse(new SortField("field", true).equals(null));
        assertFalse(new SortField("field", true).equals(""));
        assertEquals(new SortField("field", true), new SortField("field", true));
        assertFalse(new SortField("field1", true).equals(new SortField("field2", true)));
        assertFalse(new SortField("field", true).equals(new SortField("field", false)));
    }

    @Test
    public void testToString() {
        SortField sortField = new SortField("field", null);
        assertEquals("SortField{field=field, reverse=false}", sortField.toString());
    }

    @Test
    public void testToJson() throws IOException {
        SortField sortField = new SortField("field", null);
        String expectedJson = "{field:\"field\",reverse:false}";
        assertEquals(expectedJson, JsonSerializer.toString(sortField));
        assertEquals(expectedJson, JsonSerializer.toString(JsonSerializer.fromString(expectedJson, SortField.class)));
    }
}
