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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

public class DateRangeMapperTest {

    private static final String SHORT_PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat ssdf = new SimpleDateFormat(SHORT_PATTERN);
    private static final SimpleDateFormat lsdf = new SimpleDateFormat(DateParser.DEFAULT_PATTERN);

    @Test
    public void testConstructorWithDefaultArgs() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", null);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("to", mapper.getFrom());
        assertEquals("from", mapper.getTo());
        assertEquals(DateParser.DEFAULT_PATTERN, mapper.getPattern());
        assertNotNull(mapper.getStrategy());
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", "yyyy/MM/dd");
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("to", mapper.getFrom());
        assertEquals("from", mapper.getTo());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
        assertNotNull(mapper.getStrategy());
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullFrom() {
        new DateRangeMapper("field", "to", null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyFrom() {
        new DateRangeMapper("field", "to", "", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankFrom() {
        new DateRangeMapper("field", "to", " ", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTo() {
        new DateRangeMapper("field", "to", null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTo() {
        new DateRangeMapper("field", "to", "", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTo() {
        new DateRangeMapper("field", "to", " ", null);
    }

    @Test()
    public void testReadFromFromIntColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromLongColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromFloatColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5.3f, FloatType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromDoubleColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5.3D, DoubleType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readFrom(columns));
    }

    @Test
    public void testGetFromFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "2015/02/28 01:02:03.004 GMT", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(lsdf.parse("2015/02/28 01:02:03.004 GMT"), mapper.readFrom(columns));
    }

    @Test
    public void testGetFromFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", "yyyy-MM-dd");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(ssdf.parse("2015-02-28"), mapper.readFrom(columns));
    }

    @Test(expected = IndexException.class)
    public void testGetFromFromUnparseableStringColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        mapper.readFrom(columns);
    }

    @Test
    public void testGetFromWithNullColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertNull(mapper.readFrom(columns));
    }

    @Test()
    public void testReadToFromIntColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromLongColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5L, LongType.instance, false));
        assertEquals(new Date(5), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromFloatColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5.3f, FloatType.instance, false));
        assertEquals(new Date(5), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromDoubleColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5.3D, DoubleType.instance, false));
        assertEquals(new Date(5), mapper.readTo(columns));
    }

    @Test
    public void testGetToFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "2015/02/28 01:02:03.004 GMT", UTF8Type.instance, false));
        assertEquals(lsdf.parse("2015/02/28 01:02:03.004 GMT"), mapper.readTo(columns));
    }

    @Test
    public void testGetToFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", "yyyy-MM-dd");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "2015-02-28", UTF8Type.instance, false));
        assertEquals(ssdf.parse("2015-02-28"), mapper.readTo(columns));
    }

    @Test(expected = IndexException.class)
    public void testGetToFromUnparseableStringColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "abc", UTF8Type.instance, false));
        mapper.readTo(columns);
    }

    @Test
    public void testGetToWithNullColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        assertNull(mapper.readTo(columns));
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", null);
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFields() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 20, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 30, Int32Type.instance, false));

        Document document = new Document();
        mapper.addFields(document, columns);
        IndexableField[] indexableFields = document.getFields("field");
        assertEquals(1, indexableFields.length);
        assertTrue(indexableFields[0] instanceof Field);
        assertEquals("field", indexableFields[0].name());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals(0, document.getFields().size());
    }

    @Test
    public void testExtractAnalyzers() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testValidate() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new DateRangeMapper("field", "from", "to", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new DateRangeMapper("field", "from", "to", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new DateRangeMapper("field", "from", "to", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "from", "to", null).validate(metadata);
    }

    @Test
    public void testToString() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", "yyyy/MM/dd");
        String exp = "DateRangeMapper{name=field, from=to, to=from, pattern=yyyy/MM/dd}";
        assertEquals(exp, mapper.toString());
    }
}
