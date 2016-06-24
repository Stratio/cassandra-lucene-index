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
package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.schema.mapping.builder.DateMapperBuilder;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.dateMapper;
import static com.stratio.cassandra.lucene.util.DateParser.DEFAULT_PATTERN;
import static com.stratio.cassandra.lucene.util.DateParser.TIMESTAMP_PATTERN;
import static org.junit.Assert.*;

public class DateMapperTest extends AbstractMapperTest {

    private static final String PATTERN = "yyyy-MM-dd";
    private static final String LONG_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

    @Test
    public void testConstructorWithoutArgs() {
        DateMapper mapper = new DateMapperBuilder().build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Column date pattern is not set to default value", DEFAULT_PATTERN, mapper.parser.columnPattern);
        assertEquals("Field date pattern is not set to default value", DEFAULT_PATTERN, mapper.parser.lucenePattern);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateMapper mapper = dateMapper().validated(true)
                                        .column("column")
                                        .pattern(TIMESTAMP_PATTERN)
                                        .columnPattern("yyyy-MM-dd")
                                        .lucenePattern("yyyy/MM/dd")
                                        .build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Column date pattern is not set to default value", "yyyy-MM-dd", mapper.parser.columnPattern);
        assertEquals("Field date pattern is not set to default value", "yyyy/MM/dd", mapper.parser.lucenePattern);
    }

    @Test
    public void testJsonSerialization() {
        DateMapperBuilder builder = dateMapper().validated(true).column("column").pattern("yyyy-MM-dd");
        testJson(builder, "{type:\"date\",validated:true,column:\"column\",pattern:\"yyyy-MM-dd\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DateMapperBuilder builder = dateMapper();
        testJson(builder, "{type:\"date\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithWrongPattern() {
        dateMapper().pattern("hello").build("name");
    }

    @Test
    public void testBaseClass() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertEquals("Base class is wrong", Long.class, mapper.base);
    }

    @Test
    public void testSortField() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        SortField sortField = mapper.sortField("name", true);
        assertNotNull("SortField is not built", sortField);
        assertTrue("SortField reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testBaseValueNull() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test
    public void testBaseColumnNull() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertNull("Base column is not properly parsed", mapper.base(null));
    }

    @Test
    public void testBaseValueDate() throws ParseException {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Date date = new Date();
        long parsed = mapper.base("test", date);
        long expected = sdf.parse(sdf.format(date)).getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseColumnDate() throws ParseException {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Date date = new Date();
        Column<Date> column = Column.buildComposed("f", date, TimestampType.instance);
        Long expected = sdf.parse(sdf.format(date)).getTime();
        assertEquals("Base column is not properly parsed", expected, mapper.base(column));
    }

    @Test
    public void testBaseValueInteger() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnInteger() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Integer> column = Column.buildComposed("f", 3, Int32Type.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueLong() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3L);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnLong() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Long> column = Column.buildComposed("f", 3L, LongType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueFloatWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnFloatWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Float> column = Column.buildComposed("f", 3f, FloatType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueFloatWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.5f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnFloatWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Float> column = Column.buildComposed("f", 3.5f, FloatType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueFloatWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.6f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnFloatWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Float> column = Column.buildComposed("f", 3.6f, FloatType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueDoubleWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnDoubleWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Double> column = Column.buildComposed("f", 3d, DoubleType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueDoubleWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.5d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnDoubleWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Double> column = Column.buildComposed("f", 3.5d, DoubleType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueDoubleWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.6d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testBaseColumnDoubleWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<Double> column = Column.buildComposed("f", 3.6d, DoubleType.instance);
        assertEquals("Base column is not properly parsed", Long.valueOf(3), mapper.base(column));
    }

    @Test
    public void testBaseValueStringWithPattern() throws ParseException {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        long parsed = mapper.base("test", "2014-03-19");
        assertEquals("Base value is not properly parsed", sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test
    public void testBaseColumnStringWithPattern() throws ParseException {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Column<String> column = Column.buildComposed("f", "2014-03-19", UTF8Type.instance);
        assertEquals("Base column is not properly parsed",
                     Long.valueOf(sdf.parse("2014-03-19").getTime()),
                     mapper.base(column));
    }

    @Test
    public void testBaseValueStringWithBothPatterns() throws ParseException {
        DateMapper mapper = dateMapper().columnPattern(LONG_PATTERN).lucenePattern(PATTERN).build("name");
        long parsed = mapper.base("test", "2014-03-19 01:02:03");
        assertEquals("Base value is not properly parsed", sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test
    public void testBaseColumnStringWithBothPatterns() throws ParseException {
        DateMapper mapper = dateMapper().columnPattern(LONG_PATTERN).lucenePattern(PATTERN).build("name");
        Column<String> column = Column.buildComposed("f", "2014-03-19 01:02:03", UTF8Type.instance);
        assertEquals("Base column is not properly parsed",
                     Long.valueOf(sdf.parse("2014-03-19").getTime()),
                     mapper.base(column));
    }

    @Test(expected = IndexException.class)
    public void testBaseValueStringWithPatternInvalid() {
        dateMapper().pattern(PATTERN).build("name").base("test", "2014/03/19");
    }

    @Test(expected = IndexException.class)
    public void testBaseColumnStringWithPatternInvalid() {
        dateMapper().pattern(PATTERN).build("name").base(Column.buildComposed("n", "2014/03/19", UTF8Type.instance));
    }

    @Test(expected = IndexException.class)
    public void testBaseValueStringWithColumnPatternInvalid() {
        dateMapper().columnPattern(PATTERN).build("name").base("test", "2014/03/19");
    }

    @Test(expected = IndexException.class)
    public void testBaseColumnStringWithColumnPatternInvalid() {
        dateMapper().columnPattern(PATTERN)
                    .build("name")
                    .base(Column.buildComposed("n", "2014/03/19", UTF8Type.instance));
    }

    @Test
    public void testBaseValueStringWithoutPattern() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        long parsed = mapper.base("test", "2014/03/19 00:00:00.000 GMT");
        assertEquals("Base value is not properly parsed",
                     new DateParser(null).parse("2014/03/19 00:00:00.000 GMT").getTime(),
                     parsed);
    }

    @Test
    public void testBaseColumnStringWithoutPattern() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        assertEquals("Base value is not properly parsed",
                     Long.valueOf(new DateParser(null).parse("2014/03/19 00:00:00.000 GMT").getTime()),
                     mapper.base(Column.buildComposed("n", "2014/03/19 00:00:00.000 GMT", UTF8Type.instance)));
    }

    @Test(expected = IndexException.class)
    public void testBaseValueStringWithoutPatternInvalid() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        mapper.base("test", "2014-03-19");
    }

    @Test(expected = IndexException.class)
    public void testBaseColumnStringWithoutPatternInvalid() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        mapper.base("test", Column.buildComposed("n", "2014-03-19", UTF8Type.instance));
    }

    @Test
    public void testBaseValueTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", UUIDGen.getTimeUUID(1000));
        assertEquals("Base value is not properly parsed", Long.valueOf(1000), parsed);
    }

    @Test
    public void testBaseColumnTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<UUID> column = Column.buildComposed("n", UUIDGen.getTimeUUID(1000), TimeUUIDType.instance);
        assertEquals("Base value is not properly parsed", Long.valueOf(1000), mapper.base(column));
    }

    @Test
    public void testBaseColumnTimeUUIDGeneric() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Column<UUID> column = Column.buildComposed("n", UUIDGen.getTimeUUID(1000), UUIDType.instance);
        assertEquals("Base value is not properly parsed", Long.valueOf(1000), mapper.base(column));
    }

    @Test
    public void testBaseValueTimeUUIDTruncating() throws ParseException {
        DateMapper mapper = dateMapper().columnPattern(TIMESTAMP_PATTERN).lucenePattern(PATTERN).build("name");
        Date inputDate = new SimpleDateFormat(LONG_PATTERN).parse("2014-03-19 01:02:03");
        Date outputDate = new SimpleDateFormat(PATTERN).parse("2014-03-19");
        Long parsed = mapper.base("test", UUIDGen.getTimeUUID(inputDate.getTime()));
        assertEquals("Base value is not properly parsed", Long.valueOf(outputDate.getTime()), parsed);
    }

    @Test
    public void testBaseColumnTimeUUIDTruncating() throws ParseException {
        DateMapper mapper = dateMapper().columnPattern(TIMESTAMP_PATTERN).lucenePattern(PATTERN).build("name");
        Date inputDate = new SimpleDateFormat(LONG_PATTERN).parse("2014-03-19 01:02:03");
        Date outputDate = new SimpleDateFormat(PATTERN).parse("2014-03-19");
        Long parsed = mapper.base(Column.buildComposed("name",
                                                       UUIDGen.getTimeUUID(inputDate.getTime()),
                                                       UUIDType.instance));
        assertEquals("Base value is not properly parsed", Long.valueOf(outputDate.getTime()), parsed);
    }

    @Test(expected = IndexException.class)
    public void testBaseValueNotTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        mapper.base("name", UUID.randomUUID());
    }

    @Test(expected = IndexException.class)
    public void testBaseColumnNotTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        mapper.base(Column.buildComposed("name", UUID.randomUUID(), UUIDType.instance));
    }

    @Test
    public void testIndexedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Field field = mapper.indexedField("name", time)
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field value is wrong", time, field.numericValue().longValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Field field = mapper.sortedField("name", time)
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        DateMapper mapper = dateMapper().validated(true).pattern(PATTERN).build("name");
        assertEquals("Method #toString is wrong",
                     "DateMapper{field=name, validated=true, column=name, pattern=yyyy-MM-dd}",
                     mapper.toString());
    }
}
