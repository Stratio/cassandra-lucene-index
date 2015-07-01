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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

/**
 * @author eduardoalonso <eduardoalonso@stratio.com>
 */
public class BitemporalMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("vtFrom", mapper.getVtFrom());
        assertEquals("vtTo", mapper.getVtTo());
        assertEquals("ttFrom", mapper.getTtFrom());
        assertEquals("ttTo", mapper.getTtTo());
        assertEquals(BitemporalMapper.DEFAULT_PATTERN, mapper.getPattern());
        for (int i = 0; i <= 3; i++) {
            assertNotNull(mapper.getStrategy(i, true));
            assertNotNull(mapper.getStrategy(i, false));
            assertNotNull(mapper.getTree(i, true));
            assertNotNull(mapper.getTree(i, false));
        }

    }

    @Test
    public void testConstructorWithAllArgs() {

        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy/MM/dd");
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("vtFrom", mapper.getVtFrom());
        assertEquals("vtTo", mapper.getVtTo());
        assertEquals("ttFrom", mapper.getTtFrom());
        assertEquals("ttTo", mapper.getTtTo());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
        for (int i = 0; i <= 3; i++) {
            assertNotNull(mapper.getStrategy(i, true));
            assertNotNull(mapper.getStrategy(i, false));
            assertNotNull(mapper.getTree(i, true));
            assertNotNull(mapper.getTree(i, false));
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullVtFrom() {
        new BitemporalMapper("field", null, "vtTo", "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyVtFrom() {
        new BitemporalMapper("field", "", "vtTo", "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankVtFrom() {
        new BitemporalMapper("field", " ", "vtTo", "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullVtTo() {
        new BitemporalMapper("field", "vtFrom", null, "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyVtTo() {
        new BitemporalMapper("field", "vtFrom", "", "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankVtTo() {
        new BitemporalMapper("field", "vtFrom", " ", "ttFrom", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullTtFrom() {
        new BitemporalMapper("field", "vtFrom", "vtTo", null, "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyTtFrom() {
        new BitemporalMapper("field", "vtFrom", "vtTo", "", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankTtFrom() {
        new BitemporalMapper("field", "vtFrom", "vtTo", " ", "ttTo", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullTtTo() {
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", null, "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyTtTo() {
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankTtTo() {
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", " ", "yyyy/MM/dd");
    }

    @Test()
    public void testReadVtFromFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("vtTo", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttTo", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtFromFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vtTo", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttTo", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtFrom"));
    }

    @Test()
    public void testReadVtToFieldsFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vtTo", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttTo", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vtTo", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vtTo", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vtTo", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadVtToFieldsFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vtTo", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttTo", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vtTo"));
    }

    @Test()
    public void testReadTtFromFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vtTo", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttTo", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttTo", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtFromFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vtTo", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttTo", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttFrom"));
    }

    @Test()
    public void testReadTtToFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vtTo", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttTo", 5, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("ttTo", 5L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigInteger.valueOf(5), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("ttTo", 5.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vtTo", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttFrom", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("ttTo", 5.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vtTo", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttFrom", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("ttTo", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test()
    public void testReadTtToFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vtFrom",
                                                       "vtTo",
                                                       "ttFrom",
                                                       "ttTo",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vtTo", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttFrom", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("ttTo", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "ttTo"));
    }

    @Test
    public void testGetVtFromStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "vtFrom").getTime());
    }

    @Test
    public void testGetVtFromStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "vtFrom").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "vtFrom");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        mapper.readBitemporalDate(new Columns(), "vtFrom");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromWithNegativeColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("vtTo", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("ttTo", -1, Int32Type.instance, false));

        mapper.readBitemporalDate(columns, "vtFrom");
    }

    @Test
    public void testGetVtToStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "vtTo").getTime());
    }

    @Test
    public void testGetVtToStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "vtTo").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "vtTo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtToWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        mapper.readBitemporalDate(new Columns(), "vtTo");
    }

    @Test
    public void testGetTtFromStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "ttFrom").getTime());
    }

    @Test
    public void testGetTtFromStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "ttFrom").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "ttFrom");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtFromWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        mapper.readBitemporalDate(new Columns(), "ttFrom");
    }

    @Test
    public void testGetTtToStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "ttTo").getTime());
    }

    @Test
    public void testGetTtToStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "ttTo").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "ttTo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtToWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        mapper.readBitemporalDate(new Columns(), "ttTo");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSortField() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        mapper.sortField(false);
    }

    private void testAddFieldsOnlyThese(Document doc,
                                        String[] wishedIndexedFieldNames,
                                        String[] nonWishedIndexedFieldNames) {
        for (int i = 0; i < wishedIndexedFieldNames.length; i++) {
            IndexableField[] indexableFields = doc.getFields(wishedIndexedFieldNames[i]);
            assertEquals(1, indexableFields.length);
            assertTrue(indexableFields[0] instanceof Field);
            assertEquals(wishedIndexedFieldNames[i], indexableFields[0].name());
        }

        for (int i = 0; i < nonWishedIndexedFieldNames.length; i++) {
            IndexableField[] indexableFields = doc.getFields(nonWishedIndexedFieldNames[i]);
            assertEquals(0, indexableFields.length);
        }
    }

    @Test
    public void testAddFieldsT1() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", Long.MAX_VALUE, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", Long.MAX_VALUE, LongType.instance, false));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.t1_v", "field.t1_t"},
                               new String[]{"field.t2_v",
                                            "field.t2_t",
                                            "field.t3_v",
                                            "field.t3_t",
                                            "field.t4_v",
                                            "field.t4_t"});
    }

    @Test
    public void testAddFieldsT2() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", Long.MAX_VALUE, LongType.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.t2_v", "field.t2_t"},
                               new String[]{"field.t1_v",
                                            "field.t1_t",
                                            "field.t3_v",
                                            "field.t3_t",
                                            "field.t4_v",
                                            "field.t4_t"});
    }

    @Test
    public void testAddFieldsT3() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", Long.MAX_VALUE, LongType.instance, false));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.t3_v", "field.t3_t"},
                               new String[]{"field.t1_v",
                                            "field.t1_t",
                                            "field.t2_v",
                                            "field.t2_t",
                                            "field.t4_v",
                                            "field.t4_t"});
    }

    @Test
    public void testAddFieldsT4() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vtFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vtTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttFrom", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("ttTo", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        Document document = new Document();
        mapper.addFields(document, columns);
        testAddFieldsOnlyThese(document,
                               new String[]{"field.t4_v", "field.t4_t"},
                               new String[]{"field.t1_v",
                                            "field.t1_t",
                                            "field.t2_v",
                                            "field.t2_t",
                                            "field.t3_v",
                                            "field.t3_t"});
    }

    @Test
    public void testExtractAnalyzers() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testValidate() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vtFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vtTo"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("ttFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("ttTo"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vtFrom"),
                                                UUIDType.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vtTo"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("ttFrom"),
                                                UUIDType.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("ttTo"), UUIDType.instance, 0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutVtFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vtTo"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("ttFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("ttTo"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutVtToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vtFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("ttFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("ttTo"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutTtFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vtFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vtTo"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("ttTo"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutTtToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vtFrom"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vtTo"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("ttFrom"),
                                                UTF8Type.instance,
                                                0));
        new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", null).validate(metadata);
    }

    @Test
    public void testParseJSONWithDefaultArgs() throws IOException {
        String json = "{fields:{temporal:{type:\"bitemporal\", vtFrom:\"vtFrom\", vtTo:\"vtTo\", ttFrom:\"ttFrom\", ttTo:\"ttTo\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        BitemporalMapper mapper = (BitemporalMapper) schema.getMapper("temporal");
        assertEquals(BitemporalMapper.class, mapper.getClass());
        assertEquals("temporal", mapper.getName());
        assertEquals("vtFrom", mapper.getVtFrom());
        assertEquals("vtTo", mapper.getVtTo());
        assertEquals("ttFrom", mapper.getTtFrom());
        assertEquals("ttTo", mapper.getTtTo());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals(BitemporalMapper.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{temporal:{type:\"bitemporal\", vtFrom:\"vtFrom\", vtTo:\"vtTo\", ttFrom:\"ttFrom\", ttTo:\"ttTo\", pattern:\"yyyy/MM/dd\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        BitemporalMapper mapper = (BitemporalMapper) schema.getMapper("temporal");
        assertEquals(BitemporalMapper.class, mapper.getClass());
        assertEquals("temporal", mapper.getName());
        assertEquals("vtFrom", mapper.getVtFrom());
        assertEquals("vtTo", mapper.getVtTo());
        assertEquals("ttFrom", mapper.getTtFrom());
        assertEquals("ttTo", mapper.getTtTo());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
    }

    @Test
    public void testToString() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vtFrom", "vtTo", "ttFrom", "ttTo", "yyyy/MM/dd");
        String exp = "BitemporalMapper{name=field, vtFrom=vtFrom, vtTo=vtTo, ttFrom=ttFrom, ttTo=ttTo, pattern=yyyy/MM/dd}";
        assertEquals(exp, mapper.toString());
    }
}
