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

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

public class DateRangeMapperTest {

    private static final String SHORT_PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat ssdf = new SimpleDateFormat(SHORT_PATTERN);
    private static final SimpleDateFormat lsdf = new SimpleDateFormat(DateRangeMapper.DEFAULT_PATTERN);

    @Test
    public void testConstructorWithDefaultArgs() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", null);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("to", mapper.getStart());
        assertEquals("from", mapper.getStop());
        assertEquals(DateRangeMapper.DEFAULT_PATTERN, mapper.getPattern());
        assertNotNull(mapper.getStrategy());
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", "yyyy/MM/dd");
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("to", mapper.getStart());
        assertEquals("from", mapper.getStop());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
        assertNotNull(mapper.getStrategy());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullStart() {
        new DateRangeMapper("field", "to", null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyStart() {
        new DateRangeMapper("field", "to", "", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankStart() {
        new DateRangeMapper("field", "to", " ", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullStop() {
        new DateRangeMapper("field", "to", null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyStop() {
        new DateRangeMapper("field", "to", "", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankStop() {
        new DateRangeMapper("field", "to", " ", null);
    }

    @Test()
    public void testReadStartFromIntColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readStart(columns));
    }

    @Test()
    public void testGetStartFromLongColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readStart(columns));
    }

    @Test()
    public void testGetStartFromFloatColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5.3f, FloatType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readStart(columns));
    }

    @Test()
    public void testGetStartFromDoubleColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 5.3D, DoubleType.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readStart(columns));
    }

    @Test
    public void testGetStartFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(lsdf.parse("2015/02/28 01:02:03.004"), mapper.readStart(columns));
    }

    @Test
    public void testGetStartFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", "yyyy-MM-dd");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        assertEquals(ssdf.parse("2015-02-28"), mapper.readStart(columns));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStartFromUnparseableStringColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        mapper.readStart(columns);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStartWithNullColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        mapper.readStart(new Columns());
    }

    @Test()
    public void testReadStopFromIntColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5, Int32Type.instance, false));
        assertEquals(new Date(5), mapper.readStop(columns));
    }

    @Test()
    public void testGetStopFromLongColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5L, LongType.instance, false));
        assertEquals(new Date(5), mapper.readStop(columns));
    }

    @Test()
    public void testGetStopFromFloatColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5.3f, FloatType.instance, false));
        assertEquals(new Date(5), mapper.readStop(columns));
    }

    @Test()
    public void testGetStopFromDoubleColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", 5.3D, DoubleType.instance, false));
        assertEquals(new Date(5), mapper.readStop(columns));
    }

    @Test
    public void testGetStopFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        assertEquals(lsdf.parse("2015/02/28 01:02:03.004"), mapper.readStop(columns));
    }

    @Test
    public void testGetStopFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", "yyyy-MM-dd");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "2015-02-28", UTF8Type.instance, false));
        assertEquals(ssdf.parse("2015-02-28"), mapper.readStop(columns));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStopFromUnparseableStringColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("to", "abc", UTF8Type.instance, false));
        mapper.readStop(columns);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStopWithNullColumn() {
        DateRangeMapper mapper = new DateRangeMapper("field", "from", "to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("to", 0, Int32Type.instance, false));
        mapper.readStop(new Columns());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSortField() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", null);
        mapper.sortField(false);
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

    @Test(expected = IllegalArgumentException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new DateRangeMapper("field", "from", "to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutStartColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new DateRangeMapper("field", "from", "to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutStopColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "from", "to", null).validate(metadata);
    }

    @Test
    public void testParseJSONWithDefaultArgs() throws IOException {
        String json = "{fields:{position:{type:\"date_range\", start:\"from\", stop:\"to\"}}}";
        Schema schema = Schema.fromJson(json);
        DateRangeMapper mapper = (DateRangeMapper) schema.getMapper("position");
        assertEquals(DateRangeMapper.class, mapper.getClass());
        assertEquals("position", mapper.getName());
        assertEquals("from", mapper.getStart());
        assertEquals("to", mapper.getStop());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals(DateRangeMapper.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{position:{type:\"date_range\", start:\"from\", stop:\"to\", pattern:\"yyyy/MM/dd\"}}}";
        Schema schema = Schema.fromJson(json);
        DateRangeMapper mapper = (DateRangeMapper) schema.getMapper("position");
        assertEquals(DateRangeMapper.class, mapper.getClass());
        assertEquals("position", mapper.getName());
        assertEquals("from", mapper.getStart());
        assertEquals("to", mapper.getStop());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
    }

    @Test
    public void testToString() {
        DateRangeMapper mapper = new DateRangeMapper("field", "to", "from", "yyyy/MM/dd");
        String exp = "DateRangeMapper{name=field, start=to, stop=from, pattern=yyyy/MM/dd}";
        assertEquals(exp, mapper.toString());
    }
}
