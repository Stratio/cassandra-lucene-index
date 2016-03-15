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
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.GeoPointMapperBuilder;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.document.Document;
import org.junit.Test;

import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoPointMapper;
import static org.junit.Assert.*;

public class GeoPointMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not properly set", mapper.indexed);
        assertFalse("Sorted is not properly set", mapper.sorted);
        assertEquals("Latitude is not properly set", "lat", mapper.latitude);
        assertEquals("Longitude is not properly set", "lon", mapper.longitude);
        assertEquals("Mapped columns are not properly set", 2, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("lat"));
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("lon"));
        assertEquals("Max levels is not properly set", GeospatialUtils.DEFAULT_GEOHASH_MAX_LEVELS, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.distanceStrategy);
        assertNotNull("Spatial strategy for bounding boxes Latitude is not properly set", mapper.bboxStrategy);
    }

    @Test
    public void testConstructorWithAllArgs() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(5).build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not properly set", mapper.indexed);
        assertFalse("Sorted is not properly set", mapper.sorted);
        assertEquals("Latitude is not properly set", "lat", mapper.latitude);
        assertEquals("Longitude is not properly set", "lon", mapper.longitude);
        assertEquals("Max levels is not properly set", 5, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.distanceStrategy);
        assertNotNull("Spatial strategy for bounding boxes Latitude is not properly set", mapper.bboxStrategy);
    }

    @Test
    public void testConstructorWithNullMaxLevels() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(null).build("field");
        assertEquals("Max levels is not properly set", GeospatialUtils.DEFAULT_GEOHASH_MAX_LEVELS, mapper.maxLevels);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithZeroLevels() {
        geoPointMapper("lat", "lon").maxLevels(0).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNegativeLevels() {
        geoPointMapper("lat", "lon").maxLevels(-1).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithTooManyLevels() {
        geoPointMapper("lat", "lon").maxLevels(25).build("field");
    }

    @Test
    public void testJsonSerialization() {
        GeoPointMapperBuilder builder = geoPointMapper("lat", "lon").maxLevels(5);
        testJson(builder, "{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\",max_levels:5}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        GeoPointMapperBuilder builder = geoPointMapper("lat", "lon");
        testJson(builder, "{type:\"geo_point\",latitude:\"lat\",longitude:\"lon\"}");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLatitude() {
        geoPointMapper(null, "lon").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyLatitude() {
        geoPointMapper("", "lon").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankLatitude() {
        geoPointMapper(" ", "lon").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLongitude() {
        geoPointMapper(null, "lon").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyLongitude() {
        geoPointMapper("", "lon").build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankLongitude() {
        geoPointMapper(" ", "lon").build("field");
    }

    @Test
    public void testGetLatitudeFromNullColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(null, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertNull("Latitude is not properly parsed", mapper.readLatitude(columns));
    }

    @Test
    public void testGetLatitudeFromIntColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(5, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromLongColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(5L, LongType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromFloatColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(5.3f, FloatType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5.3f, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromDoubleColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(5.3D, DoubleType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5.3d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed("5.3", UTF8Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5.3d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromShortColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(new Short("5"), ShortType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed("abc", UTF8Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        mapper.readLatitude(columns);
    }

    @Test
    public void testGetLatitudeWithNullColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        assertNull("Latitude is not properly parsed", mapper.readLatitude(new Columns()));
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooSmallColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed("-91", UTF8Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed("91", UTF8Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooSmallShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(new Short("-91"), ShortType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooBigShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(new Short("91"), ShortType.instance));
        columns.add(Column.builder("lon").buildWithComposed(0, Int32Type.instance));
        mapper.readLatitude(columns);
    }

    @Test
    public void testGetLongitudeFromNullColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(5, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(null, Int32Type.instance));
        assertNull("Longitude is not properly parsed", mapper.readLongitude(columns));
    }

    @Test
    public void testGetLongitudeFromIntColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(5, Int32Type.instance));
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromLongColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(5L, LongType.instance));
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromFloatColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(5.3f, FloatType.instance));
        assertEquals("Longitude is not properly parsed", 5.3f, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromDoubleColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(5.3D, DoubleType.instance));
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed("5.3", UTF8Type.instance));
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromShortColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(new Short("5"), ShortType.instance));
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed("abc", UTF8Type.instance));
        mapper.readLongitude(columns);
    }

    @Test
    public void testGetLongitudeWithNullColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        assertNull("Longitude is not properly parsed", mapper.readLongitude(new Columns()));
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithWrongColumnType() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(UUID.randomUUID(), UUIDType.instance));
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooSmallColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed("-181", UTF8Type.instance));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed("181", UTF8Type.instance));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooSmallShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(new Short("-181"), ShortType.instance));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooBigShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(0, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed(new Short("181"), ShortType.instance));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFields() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(20, Int32Type.instance));
        columns.add(Column.builder("lon").buildWithComposed("30", UTF8Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field.dist").length);
        assertEquals("Fields are not properly created", 6, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");

        Columns columns = new Columns();

        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 0, document.getFields().size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLatitude() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("lon").buildWithComposed("30", UTF8Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLongitude() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");

        Columns columns = new Columns();
        columns.add(Column.builder("lat").buildWithComposed(20, Int32Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test
    public void testExtractAnalyzers() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").validated(true).maxLevels(7).build("field");
        String exp = "GeoPointMapper{field=field, validated=true, latitude=lat, longitude=lon, maxLevels=7}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
