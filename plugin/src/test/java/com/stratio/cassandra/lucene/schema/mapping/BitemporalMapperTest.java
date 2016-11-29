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
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime;
import com.stratio.cassandra.lucene.schema.mapping.builder.BitemporalMapperBuilder;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bitemporalMapper;
import static com.stratio.cassandra.lucene.common.DateParser.DEFAULT_PATTERN;
import static org.junit.Assert.*;

/**
 * @author eduardoalonso  {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapperTest extends AbstractMapperTest {
    @Test
    public void testConstructorWithDefaultArgs() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        assertEquals("Field is not set", "f", mapper.field);
        assertEquals("vtFrom is not set", "vtFrom", mapper.vtFrom);
        assertEquals("vtTo is not set", "vtTo", mapper.vtTo);
        assertEquals("ttFrom is not set", "ttFrom", mapper.ttFrom);
        assertEquals("ttTo is not set", "ttTo", mapper.ttTo);
        assertEquals("Now value is not set to default", Long.MAX_VALUE, mapper.nowValue, 0);
        assertEquals("Date pattern is not set to default value", DEFAULT_PATTERN, mapper.parser.pattern);
    }

    @Test
    public void testConstructorWithAllArgs() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").validated(true)
                                                                                      .pattern("yyyy/MM/dd")
                                                                                      .nowValue("2021/03/11")
                                                                                      .build("f");
        assertEquals("Field is not set", "f", mapper.field);
        assertEquals("vtFrom is not set", "vtFrom", mapper.vtFrom);
        assertEquals("vtTo is not set", "vtTo", mapper.vtTo);
        assertEquals("ttFrom is not set", "ttFrom", mapper.ttFrom);
        assertEquals("ttTo is not set", "ttTo", mapper.ttTo);
        assertEquals("Date pattern is wrong", mapper.parseBitemporalDate("2021/03/11"), BitemporalDateTime.MAX);
        assertEquals("Date pattern is not set to default value", "yyyy/MM/dd", mapper.parser.pattern);
    }

    @Test
    public void testMappedColumns() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        assertEquals("Mapped columns are not properly set", 4, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("vtFrom"));
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("vtTo"));
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("ttFrom"));
        assertTrue("Mapped columns are not set", mapper.mappedColumns.contains("ttTo"));
    }

    @Test
    public void testParseJSONWithDefaultArgs() throws IOException {
        BitemporalMapperBuilder builder = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo");
        testJson(builder, "{type:\"bitemporal\",vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"}");
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        BitemporalMapperBuilder builder = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd")
                                                                                              .nowValue("2021/03/11");
        testJson(builder,
                 "{type:\"bitemporal\",vt_from:\"vtFrom\",vt_to:\"vtTo\",tt_from:\"ttFrom\",tt_to:\"ttTo\"," +
                 "pattern:\"yyyy/MM/dd\",now_value:\"2021/03/11\"}");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullVtFrom() {
        bitemporalMapper(null, "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyVtFrom() {
        bitemporalMapper("", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankVtFrom() {
        bitemporalMapper(" ", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullVtTo() {
        bitemporalMapper("vtFrom", null, "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyVtTo() {
        bitemporalMapper("vtFrom", "", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankVtTo() {
        bitemporalMapper("vtFrom", " ", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", null, "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", "", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", " ", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", null).pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", " ").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue(" ").build("f");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithInvalidNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd")
                                                            .nowValue("2021-03-11 00:00:00.001")
                                                            .build("f");
    }

    private static Date date(String pattern, String date) {
        try {
            return new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testReadField(String pattern, String expected, Object value) {
        Date date = date(pattern, expected);
        BitemporalDateTime dateTime = new BitemporalDateTime(date);
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern(pattern).build("f");
        Columns columns;
        columns = Columns.empty().add("vtFrom", value);
        assertEquals("Wrong VT from date parsing", dateTime, mapper.readBitemporalDate(columns, "vtFrom"));
        columns = Columns.empty().add("vtTo", value);
        assertEquals("Wrong VT to date parsing", dateTime, mapper.readBitemporalDate(columns, "vtTo"));
        columns = Columns.empty().add("ttFrom", value);
        assertEquals("Wrong TT from date parsing", dateTime, mapper.readBitemporalDate(columns, "ttFrom"));
        columns = Columns.empty().add("ttTo", value);
        assertEquals("Wrong TT to date parsing", dateTime, mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadFieldFromIntegerColumn() {
        testReadField("yyyyMMdd", "19821127", 19821127);
    }

    @Test
    public void testReadFieldFromLongColumn() {
        testReadField("yyyyMMdd", "19821127", 19821127L);
    }

    @Test
    public void testReadFieldFromTimeUUIDColumn() {
        Date date = date("yyyyMMdd", "19821127");
        UUID uuid = UUIDGen.getTimeUUID(date.getTime());
        testReadField("yyyyMMdd", "19821127", uuid);
    }

    @Test
    public void testReadFieldFromBigIntegerColumn() {
        testReadField("yyyyMMdd", "19821127", BigInteger.valueOf(19821127));
    }

    @Test
    public void testReadFieldFromFloatColumn() {
        testReadField("yyyyMM", "198211", 198211F);
    }

    @Test
    public void testReadFieldFromDoubleColumn() {
        testReadField("yyyyMM", "198211", 198211D);
    }

    @Test
    public void testReadFieldFromBigDecimalColumn() {
        testReadField("yyyyMMdd", "19821127", BigDecimal.valueOf(19821127));
    }

    @Test
    public void testReadFieldFromStringColumn() throws ParseException {
        testReadField("yyyyMMdd", "19821127", "19821127");
    }

    @Test
    public void testReadFieldFromDateColumn() throws ParseException {
        Date date = date("yyyyMMdd", "19821127");
        testReadField("yyyyMMdd", "19821127", date);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithVtToSmallerThanVtFromFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("y").build("f");
        Columns columns = Columns.empty().add("vtFrom", 5L)
                                       .add("vtTo", 0L)
                                       .add("ttFrom", 0L)
                                       .add("ttTo", 0L);
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithTtToSmallerThanTtFromFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("y").build("f");
        Columns columns = Columns.empty().add("vtFrom", 0L)
                                       .add("vtTo", 0L)
                                       .add("ttFrom", 5L)
                                       .add("ttTo", 0L);
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        mapper.sortField("f", false);
    }

    private void testAddFieldsOnlyThese(List<IndexableField> fields,
                                        String[] wishedIndexedFieldNames,
                                        String[] nonWishedIndexedFieldNames) {
        Document doc = new Document();
        fields.forEach(doc::add);
        for (String wishedIndexedFieldName : wishedIndexedFieldNames) {
            IndexableField[] indexableFields = doc.getFields(wishedIndexedFieldName);
            assertEquals("Add fields is wrong", 1, indexableFields.length);
            assertTrue("Add fields is wrong", indexableFields[0] instanceof Field);
            assertEquals("Add fields is wrong", wishedIndexedFieldName, indexableFields[0].name());
        }
        for (String nonWishedIndexedFieldName : nonWishedIndexedFieldNames) {
            IndexableField[] indexableFields = doc.getFields(nonWishedIndexedFieldName);
            assertEquals("Add fields is wrong", 0, indexableFields.length);
        }
    }

    @Test
    public void testAddFieldsT1() {
        String nowValue = "2100/01/01 00:00:00.001 GMT";
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue).build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", nowValue)
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", nowValue);
        List<IndexableField> fields = mapper.indexableFields(columns);
        testAddFieldsOnlyThese(fields, new String[]{"f.ttFrom", "f.ttTo", "f.vtFrom", "f.vtTo"}, new String[0]);
    }

    @Test
    public void testAddFieldsT2() {
        String nowValue = "2100/01/01 00:00:00.000 GMT";
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue).build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", nowValue);
        List<IndexableField> fields = mapper.indexableFields(columns);
        testAddFieldsOnlyThese(fields, new String[]{"f.ttFrom", "f.ttTo", "f.vtFrom", "f.vtTo"}, new String[0]);
    }

    @Test
    public void testAddFieldsT3() {
        String nowValue = "2100/01/01 00:00:00.000 GMT";
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue).build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", nowValue)
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        List<IndexableField> fields = mapper.indexableFields(columns);
        testAddFieldsOnlyThese(fields, new String[]{"f.ttFrom", "f.ttTo", "f.vtFrom", "f.vtTo"}, new String[0]);
    }

    @Test
    public void testAddFieldsT4() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        List<IndexableField> fields = mapper.indexableFields(columns);
        testAddFieldsOnlyThese(fields, new String[]{"f.ttFrom", "f.ttTo", "f.vtFrom", "f.vtTo"}, new String[0]);
    }

    @Test
    public void testAddFieldsAllNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty();
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Null columns should produce no fields", 0, fields.size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtFromNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtFromNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtFromAfterVtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.005 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtFromAfterTtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        Columns columns = Columns.empty()
                                 .add("vtFrom", "2015/02/28 01:02:03.004 GMT")
                                 .add("vtTo", "2015/02/28 01:02:03.004 GMT")
                                 .add("ttFrom", "2015/02/28 01:02:03.005 GMT")
                                 .add("ttTo", "2015/02/28 01:02:03.004 GMT");
        mapper.indexableFields(columns);
    }

    @Test
    public void testExtractAnalyzers() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("f");
        assertNull("Analyzer should be null", mapper.analyzer);
    }

    @Test
    public void testToString() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd")
                                                                                      .nowValue("2025/12/23")
                                                                                      .build("f");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        Date date = format.parse("2025/12/23");
        String exp = "BitemporalMapper{field=f, validated=false, vtFrom=vtFrom, vtTo=vtTo, ttFrom=ttFrom, " +
                     "ttTo=ttTo, pattern=yyyy/MM/dd, nowValue=" + date.getTime() + "}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
