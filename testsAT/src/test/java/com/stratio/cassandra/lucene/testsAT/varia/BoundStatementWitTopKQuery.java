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

package com.stratio.cassandra.lucene.testsAT.varia;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.builder.search.sort.SimpleSortField;
import com.stratio.cassandra.lucene.testsAT.search.AbstractSearchAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertArrayEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class BoundStatementWitTopKQuery extends AbstractSearchAT {

    private List<Row> prepareAndExecuteBoundingBox(String query) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ");
        sb.append(utils.getKeyspace());
        sb.append(".");
        sb.append(utils.getTable());
        sb.append(" ");
        sb.append(" WHERE lucene = ?");
        final PreparedStatement stmt = CassandraConnection.session.prepare(sb.toString());

        BoundStatement b = stmt.bind();
        //String query=search().query(range("double_1").lower(11).upper(50).includeLower(true).includeUpper(true)).build();
        b.setString("lucene", query);
        b.setFetchSize(2);

        return utils.execute(b).all();
    }
    private Integer[] intColumn(List<Row> rows, String name) {
        Integer[] values = new Integer[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getInt(name);
        }
        return values;
    }
    private Double[] doubleColumn(List<Row> rows, String name) {
        Double[] values = new Double[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getDouble(name);
        }
        return values;
    }

    @Test
    public void sortIntegerAsc() {
        String query=search().sort(new SimpleSortField("integer_1").reverse(false)).build();
        Integer[] returnedValues = intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDesc() {
        String query=search().sort(field("integer_1").reverse(true)).build();
        Integer[] returnedValues = intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDefault() {
        String query=search().sort(field("integer_1")).build();
        Integer[] returnedValues = intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleAsc() {
        String query=search().sort(field("double_1").reverse(false)).build();
        Double[] returnedValues = doubleColumn(prepareAndExecuteBoundingBox(query), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDesc() {
        String query=search().sort(field("double_1").reverse(true)).build();
        Double[] returnedValues = doubleColumn(prepareAndExecuteBoundingBox(query), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{3D, 3D, 3D, 2D, 1D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDefault() {
        String query=search().sort(field("double_1")).build();
        Double[] returnedValues = doubleColumn(prepareAndExecuteBoundingBox(query), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortCombined() {
        String query=search().sort(field("double_1"),field("integer_1")).build();
        List<Row> rows=prepareAndExecuteBoundingBox(query);
        Double[] returnedDoubleValues = doubleColumn(rows, "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedDoubleValues.length);
        Integer[] returnedIntValues = intColumn(rows,"integer_1") ;
        Assert.assertEquals("Expected 5 results!", 5, returnedIntValues.length);
        Double[] expectedDoubleValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        Integer[] expectedIntValues = new Integer[]{-1, -2, -5, -4, -3};
        assertArrayEquals("Wrong doubles sort!", expectedDoubleValues, returnedDoubleValues);
        assertArrayEquals("Wrong integers sort!", expectedIntValues, returnedIntValues);
    }

    @Test
    public void sortWithFilter() {
        String query = search().filter(all()).sort(field("integer_1").reverse(false)).build();
        Integer[] returnedValues =intColumn(prepareAndExecuteBoundingBox(query), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithQuery() {
        String query= search().query(all()).sort(field("integer_1").reverse(false)).build();
        Integer[] returnedValues =intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithFilterAndQuery() {
        String query = search().filter(all()).query(all()).sort(field("integer_1").reverse(false)).build();
        Integer[] returnedValues =intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterNotReversed() {
        String query =search().filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceSortField("geo_point", -3.784519, 40.442163).reverse(false)).build();
        Integer[] returnedValues =intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test public void sortWithGeoDistanceQueryNotReversed() {
        String query = search().query(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(geoDistanceSortField(
                "geo_point",
                -3.784519,
                40.442163).reverse(false)).build();
        Integer[] returnedValues = intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterReversed() {
        String query=search().filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceSortField("geo_point", -3.784519, 40.442163).reverse(true)).build();
        Integer[] returnedValues =intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceQueryReversed() {
        String query=search().query(geoDistance("geo_point", -3.784519, 40.442163, "10000km"))
                             .sort(geoDistanceSortField("geo_point", -3.784519, 40.442163).reverse(true))
                             .build();
        Integer[] returnedValues = intColumn(prepareAndExecuteBoundingBox(query),"integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }
}