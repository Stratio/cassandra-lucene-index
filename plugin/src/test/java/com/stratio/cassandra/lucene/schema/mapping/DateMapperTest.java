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
import com.stratio.cassandra.lucene.schema.mapping.builder.DateMapperBuilder;
import com.stratio.cassandra.lucene.util.DateParser;
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
import static org.junit.Assert.*;

public class DateMapperTest extends AbstractMapperTest {

    private static final String PATTERN = "yyyy-MM-dd";
    private static final String TIMESTAMP_PATTERN = "timestamp";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

    @Test
    public void testConstructorWithoutArgs() {
        DateMapper mapper = new DateMapperBuilder().build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.indexed);
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.sorted);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Pattern is not set to default value", DateParser.DEFAULT_PATTERN, mapper.pattern);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateMapper mapper = dateMapper().indexed(false)
                                        .sorted(true)
                                        .validated(true)
                                        .column("column")
                                        .pattern(PATTERN)
                                        .build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertFalse("Indexed is not properly set", mapper.indexed);
        assertTrue("Sorted is not properly set", mapper.sorted);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Pattern is not properly set", PATTERN, mapper.pattern);
    }

    @Test
    public void testJsonSerialization() {
        DateMapperBuilder builder = dateMapper().indexed(false)
                                                .sorted(true)
                                                .validated(true)
                                                .column("column")
                                                .pattern("yyyy-MM-dd");
        testJson(builder,
                 "{type:\"date\",validated:true,indexed:false,sorted:true,column:\"column\",pattern:\"yyyy-MM-dd\"}");
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
    public void testValueNull() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test
    public void testValueDate() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        Date date = new Date();
        long parsed = mapper.base("test", date);
        assertEquals("Base value is not properly parsed", date.getTime(), parsed);
    }

    @Test
    public void testValueInteger() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3);
        assertEquals("Base value is not properly parsed", Long.valueOf(3 * 24L * 60L * 60L * 1000L), parsed);
    }

    @Test
    public void testValueLong() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3l);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.5f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.6f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.5d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", 3.6d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithPattern() throws ParseException {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        long parsed = mapper.base("test", "2014-03-19");
        assertEquals("Base value is not properly parsed", sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringWithPatternInvalid() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        mapper.base("test", "2014/03/19");
    }

    @Test
    public void testValueStringWithoutPattern() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        long parsed = mapper.base("test", "2014/03/19 00:00:00.000 GMT");
        assertEquals("Base value is not properly parsed",
                     new DateParser(null).parse("2014/03/19 00:00:00.000 GMT").getTime(),
                     parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringWithoutPatternInvalid() throws ParseException {
        DateMapper mapper = dateMapper().build("name");
        mapper.base("test", "2014-03-19");
    }

    @Test
    public void testValueTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        Long parsed = mapper.base("test", UUIDGen.getTimeUUID(1000));
        assertEquals("Base value is not properly parsed", Long.valueOf(1000), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueNotTimeUUID() {
        DateMapper mapper = dateMapper().pattern(TIMESTAMP_PATTERN).build("name");
        mapper.base("test", UUID.randomUUID());
    }

    @Test
    public void testIndexedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = dateMapper().indexed(true).pattern(PATTERN).build("name");
        Field field = mapper.indexedField("name", time);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", time, field.numericValue().longValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = dateMapper().sorted(true).pattern(PATTERN).build("name");
        Field field = mapper.sortedField("name", time);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DateMapper mapper = dateMapper().pattern(PATTERN).build("name");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        DateMapper mapper = dateMapper().indexed(false).sorted(true).validated(true).pattern(PATTERN).build("name");
        assertEquals("Method #toString is wrong",
                     "DateMapper{field=name, indexed=false, sorted=true, validated=true, column=name, " +
                     "pattern=yyyy-MM-dd}",
                     mapper.toString());
    }
}
