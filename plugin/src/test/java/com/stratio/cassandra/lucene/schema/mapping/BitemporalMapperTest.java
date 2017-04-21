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
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime;
import com.stratio.cassandra.lucene.schema.mapping.builder.BitemporalMapperBuilder;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bitemporalMapper;
import static org.junit.Assert.*;

/**
 * @author eduardoalonso  {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertEquals("vtFrom is not set", "vtFrom", mapper.vtFrom);
        assertEquals("vtTo is not set", "vtTo", mapper.vtTo);
        assertEquals("ttFrom is not set", "ttFrom", mapper.ttFrom);
        assertEquals("ttTo is not set", "ttTo", mapper.ttTo);
        assertEquals("Now value is not set to default", Long.MAX_VALUE, mapper.nowValue, 0);
        assertEquals("Date pattern is not set to default", DateParser.DEFAULT_PATTERN, mapper.pattern);
    }

    @Test
    public void testConstructorWithAllArgs() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").validated(true)
                                                                                      .pattern("yyyy/MM/dd")
                                                                                      .nowValue("2021/03/11")
                                                                                      .build("field");
        assertEquals("Field is not set", "field", mapper.field);
        assertEquals("vtFrom is not set", "vtFrom", mapper.vtFrom);
        assertEquals("vtTo is not set", "vtTo", mapper.vtTo);
        assertEquals("ttFrom is not set", "ttFrom", mapper.ttFrom);
        assertEquals("ttTo is not set", "ttTo", mapper.ttTo);
        assertEquals("Date pattern is wrong", mapper.parseBitemporalDate("2021/03/11"), BitemporalDateTime.MAX);

    }

    @Test
    public void testMappedColumns() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");
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
        bitemporalMapper(null, "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyVtFrom() {
        bitemporalMapper("", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankVtFrom() {
        bitemporalMapper(" ", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullVtTo() {
        bitemporalMapper("vtFrom", null, "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyVtTo() {
        bitemporalMapper("vtFrom", "", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankVtTo() {
        bitemporalMapper("vtFrom", " ", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", null, "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", "", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTtFrom() {
        bitemporalMapper("vtFrom", "vtTo", " ", "ttTo").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", null).pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankTtTo() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", " ").pattern("yyyy/MM/dd").nowValue("2021/03/11").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue("").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd").nowValue(" ").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithInvalidNowValue() {
        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd")
                                                            .nowValue("2021-03-11 00:00:00.001")
                                                            .build("field");
    }

    @Test
    public void testReadVtFromFieldFromInt32Column() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(5, Int32Type.instance));
        columns.add(Column.builder("vtTo").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttTo").composedValue(0, Int32Type.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(5L * 24L * 60L * 60L * 1000L),
                     mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(5L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(0L, LongType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromTimeUUIDColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(UUIDGen.getTimeUUID(5L), TimeUUIDType.instance));
        columns.add(Column.builder("vtTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigInteger.valueOf(5), IntegerType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromSimpleDateColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        columns.add(Column.builder("vtFrom").decomposedValue(bb, SimpleDateType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(1444435200000L),
                     mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(5.0f, FloatType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0f, FloatType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(5.0, DoubleType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0, DoubleType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigDecimal.valueOf(5.0), DecimalType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromAsciiColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromUTF8Column() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtFromFieldFromTimeStampColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("vtTo").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttTo").composedValue(date, TimestampType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test
    public void testReadVtToFieldsFromInt32Column() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("vtTo").composedValue(5, Int32Type.instance));
        columns.add(Column.builder("ttFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttTo").composedValue(0, Int32Type.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(5L * 24L * 60L * 60L * 1000L),
                     mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(5L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(0L, LongType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromTimeUUIDColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("vtTo").composedValue(UUIDGen.getTimeUUID(5L), TimeUUIDType.instance));
        columns.add(Column.builder("ttFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromIntegerColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigInteger.valueOf(5), IntegerType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldFromSimpleDateColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        columns.add(Column.builder("vtTo").decomposedValue(bb, SimpleDateType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(1444435200000L),
                     mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromFloatColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("vtTo").composedValue(5.0f, FloatType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0f, FloatType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromDoubleColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("vtTo").composedValue(5.0, DoubleType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0, DoubleType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromDecimalColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigDecimal.valueOf(5.0), DecimalType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromAsciiColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromUTF8Column() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadVtToFieldsFromTimeStampColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("vtTo").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttTo").composedValue(date, TimestampType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test
    public void testReadTtFromFieldFromInt32Column() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("vtTo").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttFrom").composedValue(5, Int32Type.instance));
        columns.add(Column.builder("ttTo").composedValue(0, Int32Type.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(5L * 24L * 60L * 60L * 1000L),
                     mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(5L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(0L, LongType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromTimeUUIDColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("vtTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttFrom").composedValue(UUIDGen.getTimeUUID(5L), TimeUUIDType.instance));
        columns.add(Column.builder("ttTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigInteger.valueOf(5), IntegerType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromSimpleDateColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        columns.add(Column.builder("ttFrom").decomposedValue(bb, SimpleDateType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(1444435200000L),
                     mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttFrom").composedValue(5.0f, FloatType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0f, FloatType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttFrom").composedValue(5.0, DoubleType.instance));
        columns.add(Column.builder("ttTo").composedValue(0.0, DoubleType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigDecimal.valueOf(5.0), DecimalType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromAsciiColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromUTF8Column() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtFromFieldFromTimeStampColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("vtTo").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttTo").composedValue(date, TimestampType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(date),
                     mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test
    public void testReadTtToFieldFromInt32Column() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("vtTo").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttFrom").composedValue(0, Int32Type.instance));
        columns.add(Column.builder("ttTo").composedValue(5, Int32Type.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(5L * 24L * 60L * 60L * 1000L),
                     mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(5L, LongType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromTimeUUIDColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("vtTo").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttFrom").composedValue(UUIDGen.getTimeUUID(0L), TimeUUIDType.instance));
        columns.add(Column.builder("ttTo").composedValue(UUIDGen.getTimeUUID(5L), TimeUUIDType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromIntegerColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigInteger.valueOf(0), IntegerType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigInteger.valueOf(5), IntegerType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromSimpleDateColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        ByteBuffer bb = SimpleDateType.instance.fromString("2015-10-10");
        columns.add(Column.builder("ttTo").decomposedValue(bb, SimpleDateType.instance));
        assertEquals("Date parsing is wrong",
                     new BitemporalDateTime(1444435200000L),
                     mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromFloatColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0f, FloatType.instance));
        columns.add(Column.builder("ttTo").composedValue(5.0f, FloatType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromDoubleColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("vtTo").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0.0, DoubleType.instance));
        columns.add(Column.builder("ttTo").composedValue(5.0, DoubleType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromDecimalColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("vtTo").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttFrom").composedValue(BigDecimal.valueOf(0.0), DecimalType.instance));
        columns.add(Column.builder("ttTo").composedValue(BigDecimal.valueOf(5.0), DecimalType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromAsciiColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", AsciiType.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromUTF8Column() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/03/24 11:15:14", UTF8Type.instance));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testReadTtToFieldFromTimeStampColumn() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd HH:mm:ss")
                                                                                      .nowValue("2025/12/23 00:00:00")
                                                                                      .build("field");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse("2015/03/24 11:15:14");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("vtTo").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttFrom").composedValue(date, TimestampType.instance));
        columns.add(Column.builder("ttTo").composedValue(date, TimestampType.instance));
        assertEquals("Date parsing is wrong", new BitemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testGetVtFromStringColumnWithDefaultPattern() throws ParseException {

        String pattern = DateParser.DEFAULT_PATTERN;
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015/02/28 01:02:03.004 GMT");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "vt_from").toDate());
    }

    @Test
    public void testGetVtFromStringColumnWithCustomPattern() throws ParseException {

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015-02-28");

        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy-MM-dd")
                                                                                      .nowValue("2025-12-23 00:00:00")
                                                                                      .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015-02-28", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "vtFrom").toDate());
    }

    @Test(expected = IndexException.class)
    public void testGetVtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("0673679", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("8947597", UTF8Type.instance));

        mapper.readBitemporalDate(columns, "vt_from");
    }

    @Test
    public void testGetVtFromWithNullColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vt_to").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_to").composedValue(-1, Int32Type.instance));
        assertNull("Date parsing is wrong", mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test(expected = IndexException.class)
    public void testGetVtFromWithNegativeColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("vt_to").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_to").composedValue(-1, Int32Type.instance));

        mapper.readBitemporalDate(columns, "vt_from");
    }

    @Test
    public void testGetVtToStringColumnWithDefaultPattern() throws ParseException {

        String pattern = DateParser.DEFAULT_PATTERN;
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015/02/28 01:02:03.004 GMT");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "vt_to").toDate());
    }

    @Test
    public void testGetVtToStringColumnWithCustomPattern() throws ParseException {

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015-02-28");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern("yyyy-MM-dd")
                                                                                          .nowValue("2025-12-23")
                                                                                          .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015-02-28", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "vt_to").toDate());
    }

    @Test(expected = IndexException.class)
    public void testGetVtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("0673679", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("8947597", UTF8Type.instance));

        mapper.readBitemporalDate(columns, "vt_to");
    }

    @Test
    public void testGetVtToWithNullColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_to").composedValue(-1, Int32Type.instance));
        assertNull("Date parsing is wrong", mapper.readBitemporalDate(new Columns(), "vt_to"));
    }

    @Test
    public void testGetTtFromStringColumnWithDefaultPattern() throws ParseException {

        String pattern = DateParser.DEFAULT_PATTERN;
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015/02/28 01:02:03.004 GMT");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "tt_from").toDate());
    }

    @Test
    public void testGetTtFromStringColumnWithCustomPattern() throws ParseException {

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015-02-28");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern("yyyy-MM-dd")
                                                                                          .nowValue("2025-12-23")
                                                                                          .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015-02-28", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "tt_from").toDate());
    }

    @Test(expected = IndexException.class)
    public void testGetTtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("0673679", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("8947597", UTF8Type.instance));

        mapper.readBitemporalDate(columns, "tt_from");
    }

    @Test
    public void testGetTtFromWithNullColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("vt_to").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_to").composedValue(-1, Int32Type.instance));
        assertNull("Date parsing is wrong", mapper.readBitemporalDate(new Columns(), "tt_from"));
    }

    @Test
    public void testGetTtToStringColumnWithDefaultPattern() throws ParseException {

        String pattern = DateParser.DEFAULT_PATTERN;
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015/02/28 01:02:03.004 GMT");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "tt_to").toDate());
    }

    @Test
    public void testGetTtToStringColumnWithCustomPattern() throws ParseException {

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date expectedDate = sdf.parse("2015-02-28");

        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern("yyyy-MM-dd")
                                                                                          .nowValue("2025-12-23")
                                                                                          .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("2015-02-28", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("2015-02-28", UTF8Type.instance));

        assertEquals("Date parsing is wrong", expectedDate, mapper.readBitemporalDate(columns, "tt_to").toDate());
    }

    @Test(expected = IndexException.class)
    public void testGetTtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("vt_to").composedValue("0673679", UTF8Type.instance));
        columns.add(Column.builder("tt_from").composedValue("abc", UTF8Type.instance));
        columns.add(Column.builder("tt_to").composedValue("8947597", UTF8Type.instance));

        mapper.readBitemporalDate(columns, "tt_to");
    }

    @Test
    public void testGetTtToWithNullColumn() {
        BitemporalMapper mapper = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vt_from").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("vt_to").composedValue(-1, Int32Type.instance));
        columns.add(Column.builder("tt_from").composedValue(-1, Int32Type.instance));
        assertNull("Date parsing is wrong", mapper.readBitemporalDate(new Columns(), "tt_to"));
    }

    @Test(expected = IndexException.class)
    public void testContructBitemporalVtToSmallerThanVtFromFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(5L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(0L, LongType.instance));
        Document document = new Document();
        mapper.addFields(document, columns);

    }

    @Test(expected = IndexException.class)
    public void testContructBitemporalTtToSmallerThanTtFromFromLongColumn() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("timestamp")
                                                                                      .build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue(0L, LongType.instance));
        columns.add(Column.builder("vtTo").composedValue(0L, LongType.instance));
        columns.add(Column.builder("ttFrom").composedValue(5L, LongType.instance));
        columns.add(Column.builder("ttTo").composedValue(0L, LongType.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");
        mapper.sortField("field", false);
    }

    private void testAddFieldsOnlyThese(Document doc,
                                        String[] wishedIndexedFieldNames,
                                        String[] nonWishedIndexedFieldNames) {
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
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue)
                                                                                      .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue(nowValue, UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue(nowValue, UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.ttFrom", "field.ttTo", "field.vtFrom", "field.vtTo"},
                               new String[0]);
    }

    @Test
    public void testAddFieldsT2() {
        String nowValue = "2100/01/01 00:00:00.000 GMT";
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue)
                                                                                      .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue(nowValue, UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.ttFrom", "field.ttTo", "field.vtFrom", "field.vtTo"},
                               new String[0]);
    }

    @Test
    public void testAddFieldsT3() {
        String nowValue = "2100/01/01 00:00:00.000 GMT";
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").nowValue(nowValue)
                                                                                      .build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue(nowValue, UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.ttFrom", "field.ttTo", "field.vtFrom", "field.vtTo"},
                               new String[0]);
    }

    @Test
    public void testAddFieldsT4() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.ttFrom", "field.ttTo", "field.vtFrom", "field.vtTo"},
                               new String[0]);
    }

    @Test
    public void testAddFieldsAllNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");
        Columns columns = new Columns();
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Null columns should produce no fields", 0, document.getFields().size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtFromNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtFromNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsVtFromAfterVtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.005 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsTtFromAfterTtToNull() {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("vtFrom").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("vtTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttFrom").composedValue("2015/02/28 01:02:03.005 GMT", UTF8Type.instance));
        columns.add(Column.builder("ttTo").composedValue("2015/02/28 01:02:03.004 GMT", UTF8Type.instance));

        mapper.addFields(new Document(), columns);
    }

    @Test
    public void testExtractAnalyzers() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").build("field");
        assertNull("Analyzer should be null", mapper.analyzer);
    }

    @Test
    public void testToString() throws ParseException {
        BitemporalMapper mapper = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern("yyyy/MM/dd")
                                                                                      .nowValue("2025/12/23")
                                                                                      .build("field");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        Date date = format.parse("2025/12/23");
        String exp = "BitemporalMapper{field=field, validated=false, vtFrom=vtFrom, vtTo=vtTo, ttFrom=ttFrom, " +
                     "ttTo=ttTo, pattern=yyyy/MM/dd, nowValue=" + date.getTime() + "}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
