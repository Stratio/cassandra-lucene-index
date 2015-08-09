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
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.document.Document;
import org.junit.Test;

import java.util.UUID;

import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

public class GeoPointMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("lat", mapper.getLatitude());
        assertEquals("lon", mapper.getLongitude());
        assertEquals(GeoPointMapper.DEFAULT_MAX_LEVELS, mapper.getMaxLevels());
        assertNotNull(mapper.getDistanceStrategy());
    }

    @Test
    public void testConstructorWithAllArgs() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 7);
        assertEquals("field", mapper.getName());
        assertTrue(mapper.isIndexed());
        assertFalse(mapper.isSorted());
        assertEquals("lat", mapper.getLatitude());
        assertEquals("lon", mapper.getLongitude());
        assertEquals(7, mapper.getMaxLevels());
        assertNotNull(mapper.getDistanceStrategy());
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLatitude() {
        new GeoPointMapper("field", null, "lon", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyLatitude() {
        new GeoPointMapper("field", "", "lon", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankLatitude() {
        new GeoPointMapper("field", " ", "lon", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLongitude() {
        new GeoPointMapper("field", "lat", null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyLongitude() {
        new GeoPointMapper("field", "lat", "", null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankLongitude() {
        new GeoPointMapper("field", "lat", " ", null);
    }

    @Test()
    public void testGetLatitudeFromIntColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 5, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        assertEquals(5d, mapper.readLatitude(columns), 0);
    }

    @Test()
    public void testGetLatitudeFromLongColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 5L, LongType.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        assertEquals(5d, mapper.readLatitude(columns), 0);
    }

    @Test()
    public void testGetLatitudeFromFloatColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 5.3f, FloatType.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        assertEquals(5.3f, mapper.readLatitude(columns), 0);
    }

    @Test()
    public void testGetLatitudeFromDoubleColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 5.3D, DoubleType.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        assertEquals(5.3d, mapper.readLatitude(columns), 0);
    }

    @Test()
    public void testGetLatitudeFromStringColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", "5.3", UTF8Type.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        assertEquals(5.3d, mapper.readLatitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", "abc", UTF8Type.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        mapper.readLatitude(columns);
    }

    @Test
    public void testGetLatitudeWithNullColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        assertNull(mapper.readLatitude(new Columns()));
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooSmallColumnValue() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", "-91", UTF8Type.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", "91", UTF8Type.instance, false));
        columns.add(Column.fromComposed("lon", 0, Int32Type.instance, false));
        mapper.readLatitude(columns);
    }

    @Test()
    public void testGetLongitudeFromIntColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", 5, Int32Type.instance, false));
        assertEquals(5d, mapper.readLongitude(columns), 0);
    }

    @Test()
    public void testGetLongitudeFromLongColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", 5L, LongType.instance, false));
        assertEquals(5d, mapper.readLongitude(columns), 0);
    }

    @Test()
    public void testGetLongitudeFromFloatColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", 5.3f, FloatType.instance, false));
        assertEquals(5.3f, mapper.readLongitude(columns), 0);
    }

    @Test()
    public void testGetLongitudeFromDoubleColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", 5.3D, DoubleType.instance, false));
        assertEquals(5.3d, mapper.readLongitude(columns), 0);
    }

    @Test()
    public void testGetLongitudeFromStringColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", "5.3", UTF8Type.instance, false));
        assertEquals(5.3d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", "abc", UTF8Type.instance, false));
        mapper.readLongitude(columns);
    }

    @Test
    public void testGetLongitudeWithNullColumn() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        assertNull(mapper.readLongitude(new Columns()));
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithWrongColumnType() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", UUID.randomUUID(), UUIDType.instance, false));
        assertEquals(5.3d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooSmallColumnValue() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", "-181", UTF8Type.instance, false));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 0, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", "181", UTF8Type.instance, false));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFields() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 10);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 20, Int32Type.instance, false));
        columns.add(Column.fromComposed("lon", "30", UTF8Type.instance, false));

        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals(1, document.getFields("field.dist").length);
        assertEquals(6, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 10);

        Columns columns = new Columns();

        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals(0, document.getFields().size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLatitude() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 10);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("lon", "30", UTF8Type.instance, false));

        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLongitude() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 10);

        Columns columns = new Columns();
        columns.add(Column.fromComposed("lat", 20, Int32Type.instance, false));

        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test
    public void testExtractAnalyzers() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", null);
        assertNull(mapper.getAnalyzer());
    }

    @Test
    public void testValidate() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lat"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lon"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "lat", "lon", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {

        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lat"), UUIDType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lon"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "lat", "lon", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutLatitudeColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lon"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "lat", "lon", null).validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithouyLongitudeColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lat"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        new GeoPointMapper("field", "lat", "lon", null).validate(metadata);
    }

    @Test
    public void testToString() {
        GeoPointMapper mapper = new GeoPointMapper("field", "lat", "lon", 7);
        String exp = "GeoPointMapper{name=field, latitude=lat, longitude=lon, maxLevels=7}";
        assertEquals(exp, mapper.toString());
    }
}
