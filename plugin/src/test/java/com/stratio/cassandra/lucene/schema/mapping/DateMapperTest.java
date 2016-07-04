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
import com.stratio.cassandra.lucene.schema.mapping.builder.DateMapperBuilder;
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
import static org.junit.Assert.*;

public class DateMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithoutArgs() {
        DateMapper mapper = new DateMapperBuilder().build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Date pattern is not set to default value", DEFAULT_PATTERN, mapper.parser.pattern);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateMapper mapper = dateMapper().validated(true).column("column").pattern("yyyy-MM-dd").build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Date pattern is not set to default value", "yyyy-MM-dd", mapper.parser.pattern);
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
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        assertEquals("Base class is wrong", Long.class, mapper.base);
    }

    @Test
    public void testSortField() {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        SortField sortField = mapper.sortField("name", true);
        assertNotNull("SortField is not built", sortField);
        assertTrue("SortField reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testBaseNull() {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        assertNull("Base value is not properly parsed", mapper.base("test", null));
    }

    @Test
    public void testBaseDate() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Date date = new SimpleDateFormat("yyyyMMdd").parse("20161127");
        Long parsed = mapper.base("test", date);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseDateTruncating() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Date date = new SimpleDateFormat("yyyyMMdd HHmmss").parse("20161127 010203");
        Long parsed = mapper.base("test", date);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseInteger() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", 20161127);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseLong() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", 20161127L);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseFloat() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyy").build("name");
        Long parsed = mapper.base("test", 2016F);
        Long expected = new SimpleDateFormat("yyyy").parse("2016").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseFloatWithDecimal() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyy").build("name");
        Long parsed = mapper.base("test", 2016.3F);
        Long expected = new SimpleDateFormat("yyyy").parse("2016").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseDouble() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", 20161127D);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseDoubleWithDecimal() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", 20161127.3D);
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test
    public void testBaseString() throws ParseException {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", "20161127");
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test(expected = IndexException.class)
    public void testBaseStringInvalid() {
        dateMapper().pattern("yyyyMMdd").build("name").base("test", "2014/03/19");
    }

    @Test
    public void testBaseTimeUUID() throws ParseException {
        Long expected = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Long parsed = mapper.base("test", UUIDGen.getTimeUUID(expected));
        assertEquals("Base value is not properly parsed", expected, parsed);
    }

    @Test(expected = IndexException.class)
    public void testBaseNotTimeUUID() {
        dateMapper().pattern("yyyyMMdd").build("name").base("name", UUID.randomUUID());
    }

    @Test
    public void testIndexedField() throws ParseException {
        long base = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Field field = mapper.indexedField("name", base)
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field value is wrong", base, field.numericValue().longValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() throws ParseException {
        long base = new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime();
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        Field field = mapper.sortedField("name", base)
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DateMapper mapper = dateMapper().pattern("yyyyMMdd").build("name");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        DateMapper mapper = dateMapper().validated(true).pattern("yyyyMMdd").build("name");
        assertEquals("Method #toString is wrong",
                     "DateMapper{field=name, validated=true, column=name, pattern=yyyyMMdd}",
                     mapper.toString());
    }
}
