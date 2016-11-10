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
import com.stratio.cassandra.lucene.schema.mapping.builder.GeoShapeMapperBuilder;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import java.util.List;

import static com.stratio.cassandra.lucene.common.GeoTransformation.BBox;
import static com.stratio.cassandra.lucene.common.GeoTransformation.Centroid;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoShapeMapper;
import static org.junit.Assert.*;

public class GeoShapeMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        GeoShapeMapper mapper = geoShapeMapper().build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertEquals("Column is not properly set", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Max levels is not properly set", GeoShapeMapper.DEFAULT_MAX_LEVELS, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.strategy);
        assertNotNull("Transformations list is not properly set", mapper.transformations);
        assertTrue("Transformations list is not properly set", mapper.transformations.isEmpty());
    }

    @Test
    public void testConstructorWithAllArgs() {
        GeoShapeMapper mapper = geoShapeMapper().column("column")
                                                .validated(true)
                                                .maxLevels(10)
                                                .transformations(new Centroid(), new BBox())
                                                .build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Validated is not properly set", mapper.validated);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Max levels is not properly set", 10, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.strategy);
        assertNotNull("Transformations list is not properly set", mapper.transformations);
        assertEquals("Transformations list is not properly set", 2, mapper.transformations.size());
    }

    @Test
    public void testJsonSerialization() {
        GeoShapeMapperBuilder builder = geoShapeMapper().column("column").maxLevels(10).transformations(new Centroid());
        testJson(builder, "{type:\"geo_shape\",column:\"column\",max_levels:10,transformations:[{type:\"centroid\"}]}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        GeoShapeMapperBuilder builder = geoShapeMapper();
        testJson(builder, "{type:\"geo_shape\"}");
    }

    @Test
    public void testConstructorWithNullColumn() {
        geoShapeMapper().column(null).maxLevels(10).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyColumn() {
        geoShapeMapper().column("").maxLevels(10).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankColumn() {
        geoShapeMapper().column(" ").maxLevels(10).build("field");
    }

    @Test
    public void testConstructorWithNullMaxLevels() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(null).build("field");
        assertEquals("Max levels is not properly set", GeoShapeMapper.DEFAULT_MAX_LEVELS, mapper.maxLevels);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithZeroMaxLevels() {
        geoShapeMapper().column("column").maxLevels(0).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNegativeMaxLevels() {
        geoShapeMapper().column("column").maxLevels(-1).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithTooLargeMaxLevels() {
        geoShapeMapper().column("column").maxLevels(25).build("field");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullField() {
        geoShapeMapper().column("column").maxLevels(10).build(null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyField() {
        geoShapeMapper().column("column").maxLevels(10).build("");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankField() {
        geoShapeMapper().column("column").maxLevels(10).build(" ");
    }

    @Test
    public void testGetShapeFromString() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        assertEquals("Latitude is not properly parsed", "POLYGON", mapper.base("column", "POLYGON"));
    }

    @Test(expected = IndexException.class)
    public void testSortField() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        mapper.sortField("field", false);
    }

    @Test
    public void testAddFieldsWithValidPoint() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "POINT(30.5 10.0)");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidLineString() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "LINESTRING (30 10, 10 30, 40 40)");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidLinearRing() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "LINEARRING(30 10, 10 30, 40 40,30 10)");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidPolygon() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidPolygon2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns()
                .add("column",
                     "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidMultiPoint() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidMultiPoint2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "MULTIPOINT (10 40, 40 30, 20 20, 30 10)");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidMultiline() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns()
                .add("column", "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidUnionMultiPolygon() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns()
                .add("column",
                     "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithValidUnionMultiPolygon2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns()
                .add("column",
                     "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))");
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 2, fields.size());
    }

    @Test
    public void testAddFieldsWithNullColumns() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        List<IndexableField> fields = mapper.indexableFields(columns);
        assertEquals("Fields are not properly created", 0, fields.size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithInvalidShape() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns().add("column", "POLYON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))");
        mapper.indexableFields(columns);
    }

    @Test
    public void testExtractAnalyzers() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testToString() {
        GeoShapeMapper mapper = geoShapeMapper().validated(true)
                                                .column("column")
                                                .maxLevels(10)
                                                .transformations(new Centroid(), new BBox())
                                                .build("field");
        String exp = "GeoShapeMapper{field=field, column=column, validated=true, maxLevels=10, " +
                     "transformations=[Centroid{}, BBox{}]}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
