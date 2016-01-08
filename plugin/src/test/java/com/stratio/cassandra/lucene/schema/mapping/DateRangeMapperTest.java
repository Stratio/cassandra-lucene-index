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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.DateRangeMapperBuilder;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.dateRangeMapper;
import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

public class DateRangeMapperTest extends AbstractMapperTest {

    private static final String SHORT_PATTERN = "yyyy-MM-dd";
    private static final String TIMESTAMP_PATTERN = "timestamp";
    private static final SimpleDateFormat ssdf = new SimpleDateFormat(SHORT_PATTERN);
    private static final SimpleDateFormat lsdf = new SimpleDateFormat(DateParser.DEFAULT_PATTERN);

    @Test
    public void testConstructorWithDefaultArgs() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not set to default value", mapper.indexed);
        assertFalse("Sorted is not set to default value", mapper.sorted);
        assertEquals("From is not properly set", "from", mapper.from);
        assertEquals("To is not properly set", "to", mapper.to);
        assertEquals("Mapped columns are not properly set", 2, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("to"));
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("from"));
        assertEquals("Pattern is not set to default value", DateParser.DEFAULT_PATTERN, mapper.pattern);
        assertNotNull("Strategy is not set to default value", mapper.strategy);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern("yyyy-MM-dd").build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not properly set", mapper.indexed);
        assertFalse("Sorted is not properly set", mapper.sorted);
        assertEquals("From is not properly set", "from", mapper.from);
        assertEquals("To is not properly set", "to", mapper.to);
        assertEquals("Pattern is not properly set", "yyyy-MM-dd", mapper.pattern);
        assertNotNull("Strategy is not properly set", mapper.strategy);
    }

    @Test
    public void testJsonSerialization() {
        DateRangeMapperBuilder builder = dateRangeMapper("from", "to").pattern("yyyy-MM-dd");
        testJson(builder, "{type:\"date_range\",from:\"from\",to:\"to\",pattern:\"yyyy-MM-dd\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DateRangeMapperBuilder builder = dateRangeMapper("from", "to");
        testJson(builder, "{type:\"date_range\",from:\"from\",to:\"to\"}");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullFrom() {
        dateRangeMapper(null, "to").build("name");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyFrom() {
        dateRangeMapper("", "to").build("name");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankFrom() {
        dateRangeMapper(" ", "to").build("name");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTo() {
        dateRangeMapper("from", null).build("name");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTo() {
        dateRangeMapper("from", "").build("name");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTo() {
        dateRangeMapper("from", " ").build("name");
    }

    @Test()
    public void testReadFromFromIntColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(5, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed", new Date(5L * 24L * 60L * 60L * 1000L), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromLongColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(5L, LongType.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed", new Date(5), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromFloatColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(5.3f, FloatType.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed", new Date(5), mapper.readFrom(columns));
    }

    @Test()
    public void testGetFromFromDoubleColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(5.3D, DoubleType.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed", new Date(5), mapper.readFrom(columns));
    }

    @Test
    public void testGetFromFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed",
                     lsdf.parse("2015/02/28 01:02:03.004 GMT"),
                     mapper.readFrom(columns));
    }

    @Test
    public void testGetFromFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(SHORT_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertEquals("From is not properly parsed", ssdf.parse("2015-02-28"), mapper.readFrom(columns));
    }

    @Test(expected = IndexException.class)
    public void testGetFromFromUnparseableStringColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        mapper.readFrom(columns);
    }

    @Test
    public void testGetFromWithNullColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("to").composedValue(0, Int32Type.instance));
        assertNull("From is not properly parsed", mapper.readFrom(columns));
    }

    @Test()
    public void testReadToFromIntColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(5, Int32Type.instance));
        assertEquals("To is not properly parsed", new Date(5L * 24L * 60L * 60L * 1000L), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromLongColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(5L, LongType.instance));
        assertEquals("To is not properly parsed", new Date(5), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromFloatColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(5.3f, FloatType.instance));
        assertEquals("To is not properly parsed", new Date(5), mapper.readTo(columns));
    }

    @Test()
    public void testGetToFromDoubleColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(5.3D, DoubleType.instance));
        assertEquals("To is not properly parsed", new Date(5), mapper.readTo(columns));
    }

    @Test
    public void testGetToFromStringColumnWithDefaultPattern() throws ParseException {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        assertEquals("To is not properly parsed", lsdf.parse("2015/02/28 01:02:03.004 GMT"), mapper.readTo(columns));
    }

    @Test
    public void testGetToFromStringColumnWithCustomPattern() throws ParseException {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(SHORT_PATTERN).build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue("2015-02-28", UTF8Type.instance));
        assertEquals("To is not properly parsed", ssdf.parse("2015-02-28"), mapper.readTo(columns));
    }

    @Test(expected = IndexException.class)
    public void testGetToFromUnparseableStringColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("to").composedValue("abc", UTF8Type.instance));
        mapper.readTo(columns);
    }

    @Test
    public void testGetToWithNullColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(0, Int32Type.instance));
        assertNull("To is not properly parsed", mapper.readTo(columns));
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        DateRangeMapper mapper = dateRangeMapper("to", "from").build("field");
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFields() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");

        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(20, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(30, Int32Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
        IndexableField[] indexableFields = document.getFields("name");
        assertEquals("Indexed field is not created", 1, indexableFields.length);
        assertTrue("Indexed field type is wrong", indexableFields[0] instanceof Field);
        assertEquals("Indexed field name is wrong", "name", indexableFields[0].name());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Null columns must not produce fields", 0, document.getFields().size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithBadSortColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");

        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(2, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(1, Int32Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test
    public void testAddFieldsWithSameColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(TIMESTAMP_PATTERN).build("name");

        Columns columns = new Columns();
        columns.add(Column.builder("from").composedValue(1, Int32Type.instance));
        columns.add(Column.builder("to").composedValue(1, Int32Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
        IndexableField[] indexableFields = document.getFields("name");
        assertEquals("Indexed field is not created", 1, indexableFields.length);
    }

    @Test
    public void testExtractAnalyzers() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test(expected = IndexException.class)
    public void testValidate() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        dateRangeMapper("from", "to").build("name").validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        dateRangeMapper("from", "to").build("name").validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("to"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        dateRangeMapper("from", "to").build("name").validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("from"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        dateRangeMapper("from", "to").build("name").validate(metadata);
    }

    @Test
    public void testToString() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").validated(true).pattern("yyyy/MM/dd").build("field");
        String exp = "DateRangeMapper{field=field, validated=true, from=from, to=to, pattern=yyyy/MM/dd}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
