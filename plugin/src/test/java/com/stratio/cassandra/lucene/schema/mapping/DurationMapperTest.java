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
import com.stratio.cassandra.lucene.schema.mapping.builder.DurationMapperBuilder;
import org.apache.cassandra.cql3.Duration;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.math.BigInteger;
import java.util.*;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.durationMapper;
import static org.junit.Assert.*;

/**
 * Tests for {@link DurationMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */

public class DurationMapperTest extends AbstractMapperTest {

    @Test
    public void testNanos() {

        DurationMapper mapper = durationMapper().build("f");

        assertEquals(BigInteger.valueOf(-1), mapper.nanos("-1ns"));
        assertEquals(BigInteger.valueOf(0), mapper.nanos("0ns"));
        assertEquals(BigInteger.valueOf(1), mapper.nanos("1ns"));

        assertEquals(BigInteger.valueOf(-86400000000000L), mapper.nanos("-1d"));
        assertEquals(BigInteger.valueOf(0), mapper.nanos("0d"));
        assertEquals(BigInteger.valueOf(86400000000000L), mapper.nanos("1d"));

        assertEquals(BigInteger.valueOf(-2629800000000000L), mapper.nanos("-1mo"));
        assertEquals(BigInteger.valueOf(0), mapper.nanos("0mo"));
        assertEquals(BigInteger.valueOf(2629800000000000L), mapper.nanos("1mo"));

        assertEquals(BigInteger.valueOf(-31730403000000000L), mapper.nanos("-1y2d3s"));
        assertEquals(BigInteger.valueOf(0), mapper.nanos("0y0d0s"));
        assertEquals(BigInteger.valueOf(31730403000000000L), mapper.nanos("1y2d3s"));
    }

    @Test
    public void testSerialize() {

        DurationMapper mapper = durationMapper().build("f");

        assertEquals("00000000000000000",
                     mapper.serialize(Duration.newInstance(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE)));
        assertEquals("1grpk0a7hy2oo8wsf",
                     mapper.serialize(Duration.newInstance(Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE)));

        assertEquals("0qdus05h4dnvb0r27", mapper.serialize("-1ns"));
        assertEquals("0qdus05h4dnvb0r28", mapper.serialize("0ns"));
        assertEquals("0qdus05h4dnvb0r29", mapper.serialize("1ns"));

        assertEquals("0qdus05g9r499swe8", mapper.serialize("-1d"));
        assertEquals("0qdus05h4dnvb0r28", mapper.serialize("0d"));
        assertEquals("0qdus05hz07hc8lq8", mapper.serialize("1d"));

        assertEquals("0qdus04r86yfeg0e8", mapper.serialize("-1mo"));
        assertEquals("0qdus05h4dnvb0r28", mapper.serialize("0mo"));
        assertEquals("0qdus0670kdb7lhq8", mapper.serialize("1mo"));
    }

    @Test
    public void testComparison() {

        DurationMapper mapper = durationMapper().build("f");

        assertTrue(mapper.serialize("1ns").compareTo(mapper.serialize("1ns")) == 0);
        assertTrue(mapper.serialize("1ns").compareTo(mapper.serialize("2ns")) < 0);
        assertTrue(mapper.serialize("2ns").compareTo(mapper.serialize("1ns")) > 0);

        assertTrue(mapper.serialize("1s").compareTo(mapper.serialize("1s")) == 0);
        assertTrue(mapper.serialize("1s").compareTo(mapper.serialize("2s")) < 0);
        assertTrue(mapper.serialize("2s").compareTo(mapper.serialize("1s")) > 0);

        assertTrue(mapper.serialize("1d").compareTo(mapper.serialize("1d")) == 0);
        assertTrue(mapper.serialize("1d").compareTo(mapper.serialize("2d")) < 0);
        assertTrue(mapper.serialize("2d").compareTo(mapper.serialize("1d")) > 0);

        assertTrue(mapper.serialize("1mo").compareTo(mapper.serialize("1mo")) == 0);
        assertTrue(mapper.serialize("1mo").compareTo(mapper.serialize("2mo")) < 0);
        assertTrue(mapper.serialize("2mo").compareTo(mapper.serialize("1mo")) > 0);

        assertTrue(mapper.serialize("1mo2d").compareTo(mapper.serialize("1mo2d")) == 0);
        assertTrue(mapper.serialize("1mo3d").compareTo(mapper.serialize("2mo1d")) < 0);
        assertTrue(mapper.serialize("2mo1d").compareTo(mapper.serialize("1mo3d")) > 0);

        assertTrue(mapper.serialize("1mo").compareTo(mapper.serialize("30d")) > 0);
        assertTrue(mapper.serialize("1mo").compareTo(mapper.serialize("31d")) < 0);
        assertTrue(mapper.serialize("1mo").compareTo(mapper.serialize("100d")) < 0);

        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("364d")) > 0);
        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("365d")) > 0);
        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("366d")) < 0);

        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("11mo")) > 0);
        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("12mo")) == 0);
        assertTrue(mapper.serialize("1y").compareTo(mapper.serialize("13mo")) < 0);
    }

    @Test
    public void testConstructorWithoutArgs() {
        DurationMapper mapper = durationMapper().build("field");
        assertEquals("Field is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Column is not set to default value", "field", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("field"));
        assertEquals("Nanos in month is not default", DurationMapper.DEFAULT_NANOS_PER_MONTH, mapper.nanosPerMonth);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DurationMapper mapper = durationMapper().validated(true).column("column").nanosPerMonth(3L).build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not set", "column", mapper.column);
        assertEquals("Mapped columns are not set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("column"));
        assertEquals("Nanos in month is not default", BigInteger.valueOf(3), mapper.nanosPerMonth);
    }

    @Test
    public void testJsonSerialization() {
        testJson(durationMapper().validated(true).column("column").nanosPerMonth(2L),
                 "{type:\"duration\",validated:true,column:\"column\",nanoseconds_per_month:2}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DurationMapperBuilder builder = durationMapper();
        testJson(builder, "{type:\"duration\"}");
    }

    @Test
    public void testIndexedField() {
        DurationMapper mapper = durationMapper().build("field");
        Field field = mapper.indexedField("name", "hello")
                            .orElseThrow(() -> new AssertionError("Indexed field is not created"));
        assertEquals("Indexed field name is wrong", "name", field.name());
        assertEquals("Indexed field value is wrong", "hello", field.stringValue());
        assertFalse("Indexed field type is wrong", field.fieldType().stored());
    }

    @Test
    public void testSortedField() {
        DurationMapper mapper = durationMapper().build("field");
        Field field = mapper.sortedField("name", "hello")
                            .orElseThrow(() -> new AssertionError("Sorted field is not created"));
        assertEquals("Sorted field type is wrong", DocValuesType.SORTED_SET, field.fieldType().docValuesType());
    }

    @Test
    public void testBaseDuration() {
        DurationMapper mapper = durationMapper().build("field");
        String base = mapper.base("name", Duration.from("1y2mo3d4h5m6s"));
        assertEquals("Base case sensitiveness is wrong", "0qdus0fmc2uqffym8", base);
    }

    @Test
    public void testBaseString() {
        DurationMapper mapper = durationMapper().build("field");
        String base = mapper.base("name", "1y2mo3d4h5m6s");
        assertEquals("Base case sensitiveness is wrong", "0qdus0fmc2uqffym8", base);
    }

    @Test(expected = IndexException.class)
    public void testBaseMalformedString() {
        durationMapper().build("field").base("name", "hello");
    }

    @Test(expected = IndexException.class)
    public void testBaseNumber() {
        durationMapper().build("field").base("name", 2L);
    }

    @Test(expected = IndexException.class)
    public void testBaseBoolean() {
        durationMapper().build("field").base("name", true);
    }

    @Test(expected = IndexException.class)
    public void testBaseUUID() {
        durationMapper().build("field").base("name", UUID.randomUUID());
    }

    @Test(expected = IndexException.class)
    public void testBaseDate() {
        durationMapper().build("field").base("name", new Date());
    }

    @Test
    public void testBaseNull() {
        assertNull("Base for nulls is wrong", durationMapper().build("field").base("name", null));
    }

    @Test
    public void testAddFields() {
        DurationMapper mapper = durationMapper().build("f");
        Columns columns = Columns.empty().add("f", "1mo");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Number of created fields is wrong", 2, fields.size());
        assertTrue("Indexed field is not properly created", fields.get(0) instanceof Field);
        assertEquals("Indexed field type is wrong", KeywordMapper.FIELD_TYPE, fields.get(0).fieldType());
        assertTrue("Sorted field is not properly created", fields.get(1) instanceof SortedSetDocValuesField);
    }

    @Test
    public void testSortMonth() {
        testSort(Arrays.asList("2y", "3mo", "2mo", "4mo", "-7mo", "-5mo"),
                 Arrays.asList("-7mo", "-5mo", "2mo", "3mo", "4mo", "2y"));
    }

    @Test
    public void testSortDay() {
        testSort(Arrays.asList("1w", "0w", "-8w", "6d", "8d"),
                 Arrays.asList("-8w", "0w", "6d", "1w", "8d"));
    }

    @Test
    public void testSortNano() {
        testSort(Arrays.asList("1h", "61m", "59m", "-1h", "1us", "999ns", "1h1ns"),
                 Arrays.asList("-1h", "999ns", "1us", "59m", "1h", "1h1ns", "61m"));
    }

    private void testSort(List<String> durations, List<String> expecteds) {
        DurationMapper mapper = durationMapper().build("f");
        durations.sort(Comparator.comparing(mapper::serialize));
        assertEquals("Native and term comparisons are different", expecteds.size(), durations.size());
        for (int i = 0; i < durations.size(); i++) {
            String expected = expecteds.get(i);
            String duration = durations.get(i);
            assertEquals("Native and term comparisons are different", expected, duration);
        }
    }

    @Test
    public void testSortField() {
        DurationMapper mapper = durationMapper().build("f");
        SortField sortField = mapper.sortField("field", true);
        assertNotNull("Sort field is not created", sortField);
        assertTrue("Sort field reverse is wrong", sortField.getReverse());
    }

    @Test
    public void testExtractAnalyzers() {
        assertEquals("Method #analyzer is wrong", KeywordMapper.KEYWORD_ANALYZER, durationMapper().build("f").analyzer);
    }

    @Test
    public void testToString() {
        DurationMapper mapper = durationMapper().validated(true).nanosPerMonth(2L).build("f");
        assertEquals("Method #toString is wrong",
                     "DurationMapper{field=f, validated=true, column=f, nanosPerMonth=2}",
                     mapper.toString());
    }
}
