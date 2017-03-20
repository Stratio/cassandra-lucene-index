/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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
package com.stratio.cassandra.lucene.search.sort;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import org.apache.cassandra.db.marshal.DoubleType;
import org.junit.Test;

import java.util.Comparator;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.geoPointMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortFieldTest {

    private static SortField nullSortField() {
        return null;
    }

    private static Object nullInt() {
        return null;
    }

    @Test
    public void testBuild() {
        GeoDistanceSortField sortField = new GeoDistanceSortField("geo_place", true, 0.0, 0.0);
        assertEquals("SortField is not created", "geo_place", sortField.getMapper());
        assertTrue("SortField reverse is not set", sortField.isReverse());
        assertTrue("SortField longitude is not set", sortField.getLongitude() == 0.0);
        assertTrue("SortField latitude is not set", sortField.getLatitude() == 0.0);

    }

    @Test
    public void testBuildDefaults() {
        GeoDistanceSortField sortField = new GeoDistanceSortField("geo_place", null, 0.0, 0.0);
        assertEquals("SortField is not created", "geo_place", sortField.getMapper());
        assertEquals("SortField reverse is not set to default", SortField.DEFAULT_REVERSE, sortField.isReverse());
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new GeoDistanceSortField(null, null, 0.0, 0.0);
    }

    @Test(expected = IndexException.class)
    public void testBuildNBlankField() {
        new GeoDistanceSortField(" ", null, 0.0, 0.0);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithoutField() {
        new GeoDistanceSortField(null, null, 0.0, 0.0);
    }

    @Test
    public void testGeoDistanceSortFieldDefaults() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", null, 0.0, 0.0);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertEquals("SortField reverse is wrong", SortField.DEFAULT_REVERSE, luceneSortField.getReverse());
        assertEquals("SortField type is wrong",
                     luceneSortField.getType(),
                     org.apache.lucene.search.SortField.Type.REWRITEABLE);
    }

    @Test
    public void testGeoDistanceSortField() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", false, 0.0, 0.0);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertFalse("SortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test
    public void testGeoDistanceSortFieldReverse() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        org.apache.lucene.search.SortField luceneSortField = sortField.sortField(schema);

        assertNotNull("SortField is not created", luceneSortField);
        assertTrue("sortField reverse is wrong", luceneSortField.getReverse());
    }

    @Test(expected = IndexException.class)
    public void testGeoDistanceSortFieldUnsorted() {
        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(false)).build();
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", false, 0.0, 0.0);
        sortField.sortField(schema);
    }

    @Test(expected = IndexException.class)
    public void testGeoDistanceSortFieldWithoutMapper() {
        Schema schema = schema().build();
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", false, 0.0, 0.0);
        sortField.sortField(schema);
    }

    @Test
    public void testComparatorNatural() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", false, 0.0, 0.0);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<Double> lat = Column.builder("latitude").buildWithComposed(0.0, DoubleType.instance);
        Column<Double> lon = Column.builder("longitude").buildWithComposed(0.0, DoubleType.instance);

        Column<Double> lat2 = Column.builder("latitude").buildWithComposed(10.0, DoubleType.instance);
        Column<Double> lon2 = Column.builder("longitude").buildWithComposed(10.0, DoubleType.instance);

        Columns columns1 = new Columns().add(lat).add(lon);
        Columns columns2 = new Columns().add(lat2).add(lon2);

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorReverse() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<Double> lat = Column.builder("latitude").buildWithComposed(0.0, DoubleType.instance);
        Column<Double> lon = Column.builder("longitude").buildWithComposed(0.0, DoubleType.instance);

        Column<Double> lat2 = Column.builder("latitude").buildWithComposed(10.0, DoubleType.instance);
        Column<Double> lon2 = Column.builder("longitude").buildWithComposed(10.0, DoubleType.instance);

        Columns columns1 = new Columns().add(lat).add(lon);
        Columns columns2 = new Columns().add(lat2).add(lon2);

        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(columns1, columns1));
    }

    @Test
    public void testComparatorNullColumns() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<Double> lat = Column.builder("latitude").buildWithComposed(0.0, DoubleType.instance);
        Column<Double> lon = Column.builder("longitude").buildWithComposed(0.0, DoubleType.instance);

        Columns columns = new Columns().add(lat).add(lon);

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns, null));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(null, columns));
        assertEquals("SortField columns comparator is wrong", 0, comparator.compare(null, null));
    }

    @Test
    public void testComparatorNullColumn() {

        Schema schema = schema().mapper("field", geoPointMapper("latitude", "longitude").sorted(true)).build();

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        Comparator<Columns> comparator = sortField.comparator(schema);

        Column<Double> lat = Column.builder("latitude").buildWithComposed(0.0, DoubleType.instance);
        Column<Double> lon = Column.builder("longitude").buildWithComposed(0.0, DoubleType.instance);

        Column<Double> lat2 = Column.builder("latitude").buildWithComposed(10.0, DoubleType.instance);
        Column<Double> lon2 = Column.builder("longitude").buildWithComposed(10.0, DoubleType.instance);

        Columns columns1 = new Columns().add(lat).add(lon);
        Columns columns2 = new Columns();

        assertEquals("SortField columns comparator is wrong", -1, comparator.compare(columns1, columns2));
        assertEquals("SortField columns comparator is wrong", 1, comparator.compare(columns2, columns1));
    }

    @Test
    public void testCompareColumns() {
        GeoPointMapper mapper = geoPointMapper("latitude", "longitude").build("field");
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);

        Column<Double> lat = Column.builder("latitude").buildWithComposed(0.0, DoubleType.instance);
        Column<Double> lon = Column.builder("longitude").buildWithComposed(0.0, DoubleType.instance);

        Column<Double> lat2 = Column.builder("latitude").buildWithComposed(10.0, DoubleType.instance);
        Column<Double> lon2 = Column.builder("longitude").buildWithComposed(10.0, DoubleType.instance);

        Columns columns1 = new Columns().add(lat).add(lon);
        Columns columns2 = new Columns().add(lat2).add(lon2);
        Columns emptyColumns = new Columns();

        assertEquals("SortField compare is wrong", 1, sortField.compare(mapper, columns1, columns2));
        assertEquals("SortField compare is wrong", -1, sortField.compare(mapper, columns2, columns1));
        assertEquals("SortField compare is wrong", 1, sortField.compare(mapper, emptyColumns, columns1));
        assertEquals("SortField compare is wrong", -1, sortField.compare(mapper, columns2, emptyColumns));
        assertEquals("SortField compare is wrong", 0, sortField.compare(mapper, emptyColumns, emptyColumns));
    }

    @Test
    public void testCompareColumn() {

        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);

        Comparable column1 = "a";
        Comparable column2 = "z";

        assertEquals("SortField compare is wrong", 25, sortField.compare(column1, column2));
        assertEquals("SortField compare is wrong", -25, sortField.compare(column2, column1));
        assertEquals("SortField compare is wrong", 1, sortField.compare(null, column1));
        assertEquals("SortField compare is wrong", -1, sortField.compare(column2, null));
        assertEquals("SortField compare is wrong", 0, sortField.compare(null, null));
    }

    @Test
    public void testEquals() {
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        assertEquals("SortField equals is wrong", sortField, sortField);
        assertEquals("SortField equals is wrong", sortField, new GeoDistanceSortField("field", true, 0.0, 0.0));
        assertFalse("SortField equals is wrong", sortField.equals(new GeoDistanceSortField("field2", true, 0.0, 0.0)));
        assertFalse("SortField equals is wrong", sortField.equals(new GeoDistanceSortField("field", false, 0.0, 0.0)));
        assertFalse("SortField equals is wrong", sortField.equals(new GeoDistanceSortField("field", true, 1.0, 0.0)));
        assertFalse("SortField equals is wrong", sortField.equals(new GeoDistanceSortField("field", true, 0.0, -1.0)));
        assertFalse("SortField equals is wrong", sortField.equals(nullInt()));
        assertFalse("SortField equals is wrong", sortField.equals(nullSortField()));
    }

    @Test
    public void testEqualsWithNull() {
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        assertFalse("SortField equals is wrong", sortField.equals(null));
        assertFalse("SortField equals is wrong", sortField.equals(new Integer(0)));
    }

    @Test
    public void testHashCode() {
        assertEquals("SortField equals is wrong", -1274708409, new SimpleSortField("field", true).hashCode());
        assertEquals("SortField equals is wrong", -1274708410, new SimpleSortField("field", false).hashCode());
    }

    @Test
    public void testToString() {
        GeoDistanceSortField sortField = new GeoDistanceSortField("field", true, 0.0, 0.0);
        assertEquals("Method #toString is wrong",
                     "GeoDistanceSortField{mapper=field, reverse=true, longitude=0.0, latitude=0.0}",
                     sortField.toString());
    }
}