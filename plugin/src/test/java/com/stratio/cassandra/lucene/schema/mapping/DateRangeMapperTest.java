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
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.DateRangeMapperBuilder;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.dateRangeMapper;
import static com.stratio.cassandra.lucene.common.DateParser.DEFAULT_PATTERN;
import static org.junit.Assert.*;

public class DateRangeMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertEquals("From is not properly set", "from", mapper.from);
        assertEquals("To is not properly set", "to", mapper.to);
        assertEquals("Mapped columns are not properly set", 2, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("to"));
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("from"));
        assertEquals("Date pattern is not set to default value", DEFAULT_PATTERN, mapper.parser.pattern);
        assertNotNull("Strategy is not set to default value", mapper.strategy);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").validated(true)
                                                              .pattern("yyyyMMdd")
                                                              .build("field");
        assertEquals("Name is not properly set", "field", mapper.field);
        assertEquals("From is not properly set", "from", mapper.from);
        assertEquals("To is not properly set", "to", mapper.to);
        assertEquals("Date pattern is not set to default value", "yyyyMMdd", mapper.parser.pattern);
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

    private static void testReadField(String pattern, String expected, Object value) throws ParseException {
        Date date = new SimpleDateFormat(pattern).parse(expected);
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern(pattern).build("name");
        Columns columns;
        columns = new Columns().add("from", value);
        assertEquals("From is not properly parsed", date, mapper.readFrom(columns));
        columns = new Columns().add("to", value);
        assertEquals("To is not properly parsed", date, mapper.readTo(columns));
    }

    @Test
    public void testReadFieldFromIntColumn() throws ParseException {
        testReadField("yyyyMMdd", "20161127", 20161127);
    }

    @Test
    public void testReadFieldFromLongColumn() throws ParseException {
        testReadField("yyyyMMdd", "20161127", 20161127L);
    }

    @Test
    public void testReadFieldFromFloatColumn() throws ParseException {
        testReadField("yyyy", "2016", 2016F);
    }

    @Test
    public void testReadFieldFromDoubleColumn() throws ParseException {
        testReadField("yyyyMM", "201611", 201611D);
    }

    @Test
    public void testReadFieldFromTimeUUIDColumn() throws ParseException {
        UUID uuid = UUIDGen.getTimeUUID(new SimpleDateFormat("yyyyMMdd").parse("20161127").getTime());
        testReadField("yyyyMMdd", "20161127", uuid);
    }

    @Test(expected = IndexException.class)
    public void testReadFieldFromRandomUUIDColumn() throws ParseException {
        testReadField("yyyyMMdd", "20161127", UUID.randomUUID());
    }

    @Test
    public void testReadFieldFromString() throws ParseException {
        testReadField("yyyyMMdd", "20161127", "20161127");
    }

    @Test(expected = IndexException.class)
    public void testReadFieldFromUnparseableStringColumn() throws ParseException {
        testReadField("yyyyMMdd", "20161127", "hello");
    }

    @Test
    public void testReadFieldWithNullColumn() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        assertNull("From is not properly parsed", mapper.readFrom(new Columns()));
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        DateRangeMapper mapper = dateRangeMapper("to", "from").build("field");
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFields() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern("yyyy-MM-dd").build("name");
        Columns columns = new Columns().add("from", "1982-11-27").add("to", "2016-11-27");

        List<IndexableField> indexableFields = mapper.indexableFields(columns);
        assertEquals("Indexed field is not created", 1, indexableFields.size());
        assertTrue("Indexed field type is wrong", indexableFields.get(0) instanceof Field);
        assertEquals("Indexed field name is wrong", "name", indexableFields.get(0).name());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        Columns columns = new Columns();
        List<IndexableField> indexableFields = mapper.indexableFields(columns);
        assertEquals("Null columns must not produce fields", 0, indexableFields.size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithBadSortColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern("yyyy").build("name");
        Columns columns = new Columns().add("from", "1982").add("to", "1980");
        mapper.indexableFields(columns);
    }

    @Test
    public void testAddFieldsWithSameColumns() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").pattern("yyyy").build("name");
        Columns columns = new Columns().add("from", 2000).add("to", 2000);
        List<IndexableField> indexableFields = mapper.indexableFields(columns);
        assertEquals("Indexed field is not created", 1, indexableFields.size());
    }

    @Test
    public void testExtractAnalyzers() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").build("name");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        DateRangeMapper mapper = dateRangeMapper("from", "to").validated(true).pattern("yyyy/MM/dd").build("field");
        String exp = "DateRangeMapper{field=field, validated=true, from=from, to=to, pattern=yyyy/MM/dd}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
