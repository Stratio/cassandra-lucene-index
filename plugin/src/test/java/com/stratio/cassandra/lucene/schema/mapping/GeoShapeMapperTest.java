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
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.builder.GeoShapeMapperBuilder;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.document.Document;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoShapeMapper;
import static org.apache.cassandra.config.ColumnDefinition.regularDef;
import static org.junit.Assert.*;

public class GeoShapeMapperTest extends AbstractMapperTest {

    @Test
    public void testConstructorWithDefaultArgs() {
        GeoShapeMapper mapper = geoShapeMapper().build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not properly set", mapper.indexed);
        assertFalse("Sorted is not properly set", mapper.sorted);
        assertEquals("Column is not properly set", "field", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("field"));
        assertEquals("Max levels is not properly set", GeospatialUtils.DEFAULT_GEOHASH_MAX_LEVELS, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.strategy);
    }

    @Test
    public void testConstructorWithAllArgs() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        assertEquals("Field name is not properly set", "field", mapper.field);
        assertTrue("Indexed is not properly set", mapper.indexed);
        assertFalse("Sorted is not properly set", mapper.sorted);
        assertEquals("Column is not properly set", "column", mapper.column);
        assertEquals("Mapped columns are not properly set", 1, mapper.mappedColumns.size());
        assertTrue("Mapped columns are not properly set", mapper.mappedColumns.contains("column"));
        assertEquals("Max levels is not properly set", 10, mapper.maxLevels);
        assertNotNull("Spatial strategy for distances is not properly set", mapper.strategy);
    }

    @Test
    public void testJsonSerialization() {
        GeoShapeMapperBuilder builder = geoShapeMapper().column("column").maxLevels(10);
        testJson(builder, "{type:\"geo_shape\",column:\"column\",max_levels:10}");
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
        assertEquals("Max levels is not properly set", GeospatialUtils.DEFAULT_GEOHASH_MAX_LEVELS, mapper.maxLevels);
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

    @Test()
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
        Columns columns = new Columns();
        columns.add(Column.builder("column").composedValue("POINT(30.5 10.0)", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidLineString() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column").composedValue("LINESTRING (30 10, 10 30, 40 40)", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidLinearRing() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column").composedValue("LINEARRING(30 10, 10 30, 40 40,30 10)", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidPolygon() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidPolygon2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
                                         UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidMultiPoint() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", UTF8Type.instance));

        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidMultiPoint2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidMultiline() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))",
                                         UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidUnionMultiPolygon() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue(
                                  "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
                                  UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }

    @Test
    public void testAddFieldsWithValidUnionMultiPolygon2() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column")
                          .composedValue(
                                  "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)," +
                                  "(30 20, 20 15, 20 25, 30 20)))",
                                  UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 1, document.getFields("field").length);
        assertEquals("Fields are not properly created", 1, document.getFields().size());
    }
   
    @Test
    public void testAddFieldsWithNullColumns() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        Document document = new Document();
        mapper.addFields(document, columns);
        assertEquals("Fields are not properly created", 0, document.getFields().size());
    }

    @Test(expected = IndexException.class)
    public void testAddFieldsWithInvalidShape() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        Columns columns = new Columns();
        columns.add(Column.builder("column").composedValue("POLYON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))",
                                                           UTF8Type.instance));
        Document document = new Document();
        mapper.addFields(document, columns);
    }

    @Test
    public void testExtractAnalyzers() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        assertNull("Analyzer must be null", mapper.analyzer);
    }

    @Test
    public void testValidate() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("shape"), UTF8Type.instance, 0));
        geoShapeMapper().column("shape").maxLevels(10).build("field").validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateUnsupportedType() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("shape"), UUIDType.instance, 0));
        geoShapeMapper().column("shape").maxLevels(10).build("field").validate(metadata);
    }

    @Test(expected = IndexException.class)
    public void testValidateWithoutShapeColumn() throws ConfigurationException {
        CellNameType nameType = new SimpleSparseCellNameType(UTF8Type.instance);
        CFMetaData metadata = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, nameType);
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("lon"), FloatType.instance, 0));
        metadata.addColumnDefinition(regularDef(metadata, UTF8Type.instance.decompose("any"), UUIDType.instance, 0));
        geoShapeMapper().column("shape").maxLevels(10).build("field").validate(metadata);
    }

    @Test
    public void testToString() {
        GeoShapeMapper mapper = geoShapeMapper().column("column").maxLevels(10).build("field");
        String exp = "GeoShapeMapper{field=field, column=column, validated=false, maxLevels=10}";
        assertEquals("Method #toString is wrong", exp, mapper.toString());
    }
}
