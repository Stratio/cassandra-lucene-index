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
import com.stratio.cassandra.lucene.schema.mapping.builder.GeoPointMapperBuilder;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoPointMapper;
import static org.junit.Assert.*;

public class GeoPointMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertEquals("Validated is not set to default value", Mapper.DEFAULT_VALIDATED, mapper.validated);
        assertEquals("Latitude is not properly set", "lat", mapper.latitude);
        assertEquals("Longitude is not properly set", "lon", mapper.longitude);
        assertEquals("Mapped columns are not properly set", 2, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("lat"));
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("lon"));
        assertEquals("Max levels is not properly set", GeoPointMapper.DEFAULT_MAX_LEVELS, mapper.maxLevels);
        assertNotNull("Spatial strategy is not properly set", mapper.strategy);
    }

    @Test
    public void testConstructorWithAllArgs() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").validated(true).maxLevels(5).build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Latitude is not properly set", "lat", mapper.latitude);
        assertEquals("Longitude is not properly set", "lon", mapper.longitude);
        assertEquals("Max levels is not properly set", 5, mapper.maxLevels);
        assertNotNull("Spatial strategy is not properly set", mapper.strategy);
    }

    @Test
    public void testConstructorWithNullMaxLevels() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(null).build("field");
        assertEquals("Max levels is not properly set", GeoPointMapper.DEFAULT_MAX_LEVELS, mapper.maxLevels);
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
        Columns columns = new Columns().add("lat").add("lon", 0);
        assertNull("Latitude is not properly parsed", mapper.readLatitude(columns));
    }

    @Test
    public void testGetLatitudeFromIntColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 5).add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromLongColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 5L).add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromFloatColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 5.3f).add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5.3f, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromDoubleColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 5.3D).add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5.3d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", "5.3").add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5.3d, mapper.readLatitude(columns), 0);
    }

    @Test
    public void testGetLatitudeFromShortColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", new Short("5")).add("lon", 0);
        assertEquals("Latitude is not properly parsed", 5d, mapper.readLatitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", "abc").add("lon", 0);
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
        Columns columns = new Columns().add("lat", "-91").add("lon", 0);
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", "91").add("lon", 0);
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooSmallShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", new Short("-91")).add("lon", 0);
        mapper.readLatitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLatitudeWithTooBigShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", new Short("91")).add("lon", 0);
        mapper.readLatitude(columns);
    }

    @Test
    public void testGetLongitudeFromNullColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 5).add("lon");
        assertNull("Longitude is not properly parsed", mapper.readLongitude(columns));
    }

    @Test
    public void testGetLongitudeFromIntColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", 5);
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromLongColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", 5L);
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromFloatColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", 5.3f);
        assertEquals("Longitude is not properly parsed", 5.3f, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromDoubleColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", 5.3D);
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", "5.3");
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test
    public void testGetLongitudeFromShortColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", new Short("5"));
        assertEquals("Longitude is not properly parsed", 5d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeFromUnparseableStringColumn() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", "abc");
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
        Columns columns = new Columns().add("lat", 0).add("lon", UUID.randomUUID());
        assertEquals("Longitude is not properly parsed", 5.3d, mapper.readLongitude(columns), 0);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooSmallColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", "-181");
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooBigColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", "181");
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooSmallShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", new Short("-181"));
        mapper.readLongitude(columns);
    }

    @Test(expected = IndexException.class)
    public void testGetLongitudeWithTooBigShortColumnValue() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").build("field");
        Columns columns = new Columns().add("lat", 0).add("lon", new Short("181"));
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
        Columns columns = new Columns().add("lat", 20).add("lon", "30");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");
        Columns columns = new Columns();
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 0, fields.size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLatitude() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");
        Columns columns = new Columns().add("lon", "30");
        mapper.indexableFields(columns);
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithNullLongitude() {
        GeoPointMapper mapper = geoPointMapper("lat", "lon").maxLevels(10).build("field");
        Columns columns = new Columns().add("lat", 20);
        mapper.indexableFields(columns);
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
