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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("vt_from", mapper.getVt_from());
        assertEquals("vt_to", mapper.getVt_to());
        assertEquals("tt_from", mapper.getTt_from());
        assertEquals("tt_to", mapper.getTt_to());
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

        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy/MM/dd");
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("vt_from", mapper.getVt_from());
        assertEquals("vt_to", mapper.getVt_to());
        assertEquals("tt_from", mapper.getTt_from());
        assertEquals("tt_to", mapper.getTt_to());
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
        new BitemporalMapper("field", null, "vt_to", "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyVtFrom() {
        new BitemporalMapper("field", "", "vt_to", "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankVtFrom() {
        new BitemporalMapper("field", " ", "vt_to", "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullVtTo() {
        new BitemporalMapper("field", "vt_from", null, "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyVtTo() {
        new BitemporalMapper("field", "vt_from", "", "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankVtTo() {
        new BitemporalMapper("field", "vt_from", " ", "tt_from", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullTtFrom() {
        new BitemporalMapper("field", "vt_from", "vt_to", null, "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyTtFrom() {
        new BitemporalMapper("field", "vt_from", "vt_to", "", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankTtFrom() {
        new BitemporalMapper("field", "vt_from", "vt_to", " ", "tt_to", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullTtTo() {
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", null, "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyTtTo() {
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "", "yyyy/MM/dd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankTtTo() {
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", " ", "yyyy/MM/dd");
    }

    @Test()
    public void testReadVtFromFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("vt_to", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_to", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtFromFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vt_to", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_to", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_from"));
    }

    @Test()
    public void testReadVtToFieldsFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vt_to", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_to", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vt_to", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vt_to", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vt_to", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadVtToFieldsFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vt_to", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_to", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "vt_to"));
    }

    @Test()
    public void testReadTtFromFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vt_to", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_from", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_to", 0, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigInteger.valueOf(5), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_from", 5.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_from", 5.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_to", 0.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtFromFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vt_to", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_to", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_from"));
    }

    @Test()
    public void testReadTtToFieldFromInt32Column() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("vt_to", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_from", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_to", 5, Int32Type.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromLongColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0L, LongType.instance, false));
        columns.add(Column.fromComposed("tt_to", 5L, LongType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromIntegerColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigInteger.valueOf(0), IntegerType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigInteger.valueOf(5), IntegerType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromFloatColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0f, FloatType.instance, false));
        columns.add(Column.fromComposed("tt_to", 5.0f, FloatType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromDoubleColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("vt_to", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_from", 0.0, DoubleType.instance, false));
        columns.add(Column.fromComposed("tt_to", 5.0, DoubleType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromDecimalColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("vt_to", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_from", BigDecimal.valueOf(0.0), DecimalType.instance, false));
        columns.add(Column.fromComposed("tt_to", BigDecimal.valueOf(5.0), DecimalType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(5L), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromAsciiColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", AsciiType.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", AsciiType.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromUTF8Column() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/03/24 11:15:14", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/03/24 11:15:14", UTF8Type.instance, false));
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test()
    public void testReadTtToFieldFromTimeStampColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field",
                                                       "vt_from",
                                                       "vt_to",
                                                       "tt_from",
                                                       "tt_to",
                                                       "yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        try {
            date = format.parse("2015/03/24 11:15:14");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("vt_to", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_from", date, TimestampType.instance, false));
        columns.add(Column.fromComposed("tt_to", date, TimestampType.instance, false));
        assertEquals(new BitemporalMapper.BiTemporalDateTime(date), mapper.readBitemporalDate(columns, "tt_to"));
    }

    @Test
    public void testGetVtFromStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "vt_from").getTime());
    }

    @Test
    public void testGetVtFromStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "vt_from").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "vt_from");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        mapper.readBitemporalDate(new Columns(), "vt_from");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtFromWithNegativeColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("vt_to", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_from", -1, Int32Type.instance, false));
        columns.add(Column.fromComposed("tt_to", -1, Int32Type.instance, false));

        mapper.readBitemporalDate(columns, "vt_from");
    }

    @Test
    public void testGetVtToStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "vt_to").getTime());
    }

    @Test
    public void testGetVtToStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "vt_to").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "vt_to");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetVtToWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        mapper.readBitemporalDate(new Columns(), "vt_to");
    }

    @Test
    public void testGetTtFromStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "tt_from").getTime());
    }

    @Test
    public void testGetTtFromStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "tt_from").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtFromFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "tt_from");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtFromWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        mapper.readBitemporalDate(new Columns(), "tt_from");
    }

    @Test
    public void testGetTtToStringColumnWithDefaultPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));

        assertEquals(1425081723004L, mapper.readBitemporalDate(columns, "tt_to").getTime());
    }

    @Test
    public void testGetTtToStringColumnWithCustomPattern() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy-MM-dd");

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015-02-28", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015-02-28", UTF8Type.instance, false));

        assertEquals(1425078000000L, mapper.readBitemporalDate(columns, "tt_to").getTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtToFromUnparseableStringColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "0673679", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "8947597", UTF8Type.instance, false));

        mapper.readBitemporalDate(columns, "tt_to");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTtToWithNullColumn() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        mapper.readBitemporalDate(new Columns(), "tt_to");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSortField() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", Long.MAX_VALUE, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", Long.MAX_VALUE, LongType.instance, false));
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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", Long.MAX_VALUE, LongType.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", Long.MAX_VALUE, LongType.instance, false));
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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("vt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("vt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_from", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
        columns.add(Column.fromComposed("tt_to", "2015/02/28 01:02:03.004", UTF8Type.instance, false));
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
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null);
        String analyzer = mapper.getAnalyzer();
        assertEquals(Mapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testValidate() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vt_to"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("tt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("tt_to"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vt_from"),
                                                UUIDType.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vt_to"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("tt_from"),
                                                UUIDType.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("tt_to"), UUIDType.instance, 0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutVtFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vt_to"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("tt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("tt_to"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutVtToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("tt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("tt_to"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutTtFromColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vt_to"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("tt_to"), UTF8Type.instance, 0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateWithoutTtToColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("vt_from"),
                                                UTF8Type.instance,
                                                0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("vt_to"), UTF8Type.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata,
                                                UTF8Type.instance.decompose("tt_from"),
                                                UTF8Type.instance,
                                                0));
        new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", null).validate(metadata);
    }

    @Test
    public void testParseJSONWithDefaultArgs() throws IOException {
        String json = "{fields:{temporal:{type:\"bitemporal\", vt_from:\"vt_from\", vt_to:\"vt_to\", tt_from:\"tt_from\", tt_to:\"tt_to\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        BitemporalMapper mapper = (BitemporalMapper) schema.getMapper("temporal");
        assertEquals(BitemporalMapper.class, mapper.getClass());
        assertEquals("temporal", mapper.getName());
        assertEquals("vt_from", mapper.getVt_from());
        assertEquals("vt_to", mapper.getVt_to());
        assertEquals("tt_from", mapper.getTt_from());
        assertEquals("tt_to", mapper.getTt_to());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals(BitemporalMapper.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{temporal:{type:\"bitemporal\", vt_from:\"vt_from\", vt_to:\"vt_to\", tt_from:\"tt_from\", tt_to:\"tt_to\", pattern:\"yyyy/MM/dd\"}}}";
        Schema schema = SchemaBuilder.fromJson(json).build();
        BitemporalMapper mapper = (BitemporalMapper) schema.getMapper("temporal");
        assertEquals(BitemporalMapper.class, mapper.getClass());
        assertEquals("temporal", mapper.getName());
        assertEquals("vt_from", mapper.getVt_from());
        assertEquals("vt_to", mapper.getVt_to());
        assertEquals("tt_from", mapper.getTt_from());
        assertEquals("tt_to", mapper.getTt_to());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("yyyy/MM/dd", mapper.getPattern());
    }

    @Test
    public void testToString() {
        BitemporalMapper mapper = new BitemporalMapper("field", "vt_from", "vt_to", "tt_from", "tt_to", "yyyy/MM/dd");
        String exp = "BitemporalMapper{name=field, vt_from=vt_from, vt_to=vt_to, tt_from=tt_from, tt_to=tt_to, pattern=yyyy/MM/dd}";
        assertEquals(exp, mapper.toString());
    }
}
