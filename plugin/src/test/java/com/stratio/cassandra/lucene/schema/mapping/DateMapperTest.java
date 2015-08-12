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
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class DateMapperTest {

    private static final String PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

    @Test
    public void testConstructorWithoutArgs() {
        DateMapper mapper = new DateMapper("name", null, null, null, null);
        assertEquals("Name is not properly set", "name", mapper.getName());
        assertEquals("Indexed is not set to default value", Mapper.DEFAULT_INDEXED, mapper.isIndexed());
        assertEquals("Sorted is not set to default value", Mapper.DEFAULT_SORTED, mapper.isSorted());
        assertEquals("Pattern is not set to default value", DateParser.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateMapper mapper = new DateMapper("name", null, false, true, PATTERN);
        assertEquals("Name is not properly set", "name", mapper.getName());
        assertFalse("Indexed is not properly set", mapper.isIndexed());
        assertTrue("Sorted is not properly set", mapper.isSorted());
        assertEquals("Pattern is not properly set", PATTERN, mapper.getPattern());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithWrongPattern() {
        new DateMapper("name", null, false, false, "hello");
    }

    @Test()
    public void testBaseClass() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        assertEquals("Base class is wrong", Long.class, mapper.baseClass());
    }

    @Test()
    public void testSortField() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        SortField sortField = mapper.sortField("name", true);
        assertNotNull(sortField);
        assertTrue(sortField.getReverse());
    }

    @Test()
    public void testValueNull() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", null);
        assertNull("Base value is not properly parsed", parsed);
    }

    @Test
    public void testValueDate() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Date date = new Date();
        long parsed = mapper.base("test", date);
        assertEquals("Base value is not properly parsed", date.getTime(), parsed);
    }

    @Test
    public void testValueInteger() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3l);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3.5f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3.6f);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3.5d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        Long parsed = mapper.base("test", 3.6d);
        assertEquals("Base value is not properly parsed", Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithPattern() throws ParseException {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        long parsed = mapper.base("test", "2014-03-19");
        assertEquals("Base value is not properly parsed", sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringWithPatternInvalid() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        mapper.base("test", "2014/03/19");
    }

    @Test
    public void testValueStringWithoutPattern() throws ParseException {
        DateMapper mapper = new DateMapper("name", null, null, null, null);
        long parsed = mapper.base("test", "2014/03/19 00:00:00.000 GMT");
        assertEquals("Base value is not properly parsed",
                     new DateParser(null).parse("2014/03/19 00:00:00.000 GMT").getTime(),
                     parsed);
    }

    @Test(expected = IndexException.class)
    public void testValueStringWithoutPatternInvalid() throws ParseException {
        DateMapper mapper = new DateMapper("name", null, null, null, null);
        mapper.base("test", "2014-03-19");
    }

    @Test
    public void testIndexedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = new DateMapper("name", null, true, null, PATTERN);
        Field field = mapper.indexedField("name", time);
        assertNotNull("Indexed field is not created", field);
        assertEquals("Indexed field value is wrong", time, field.numericValue().longValue());
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field type is wrong", false, field.fieldType().stored());
    }

    @Test
    public void testSortedField() throws ParseException {
        long time = sdf.parse("2014-03-19").getTime();
        DateMapper mapper = new DateMapper("name", null, null, true, PATTERN);
        Field field = mapper.sortedField("name", time);
        assertNotNull("Sorted field is not created", field);
        assertEquals("Sorted field type is wrong", DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        DateMapper mapper = new DateMapper("name", null, null, null, PATTERN);
        assertNull("Analyzer must be null", mapper.getAnalyzer());
    }

    @Test
    public void testToString() {
        DateMapper mapper = new DateMapper("name", null, false, false, PATTERN);
        assertEquals("Method #toString is wrong",
                     "DateMapper{name=name, indexed=false, sorted=false, column=name, pattern=yyyy-MM-dd}",
                     mapper.toString());
    }
}
