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

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.sort.SimpleSortField;
import com.stratio.cassandra.lucene.testsAT.search.AbstractSearchAT;
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

    private static Integer[] intColumn(List<Row> rows, String name) {
        Integer[] values = new Integer[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getInt(name);
        }
        return values;
    }

    private static Double[] doubleColumn(List<Row> rows, String name) {
        Double[] values = new Double[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getDouble(name);
        }
        return values;
    }

    @Test
    public void sortIntegerAsc() {
        Search search = search().sort(new SimpleSortField("integer_1").reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDesc() {
        Search search = search().sort(field("integer_1").reverse(true));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDefault() {
        Search search = search().sort(field("integer_1"));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleAsc() {
        Search search = search().sort(field("double_1").reverse(false));
        Double[] returnedValues = doubleColumn(utils.searchWithPreparedStatement(search), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDesc() {
        Search search = search().sort(field("double_1").reverse(true));
        Double[] returnedValues = doubleColumn(utils.searchWithPreparedStatement(search), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{3D, 3D, 3D, 2D, 1D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDefault() {
        Search search = search().sort(field("double_1"));
        Double[] returnedValues = doubleColumn(utils.searchWithPreparedStatement(search), "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortCombined() {
        Search search = search().sort(field("double_1"), field("integer_1"));
        List<Row> rows = utils.searchWithPreparedStatement(search);
        Double[] returnedDoubleValues = doubleColumn(rows, "double_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedDoubleValues.length);
        Integer[] returnedIntValues = intColumn(rows, "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedIntValues.length);
        Double[] expectedDoubleValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        Integer[] expectedIntValues = new Integer[]{-1, -2, -5, -4, -3};
        assertArrayEquals("Wrong doubles sort!", expectedDoubleValues, returnedDoubleValues);
        assertArrayEquals("Wrong integers sort!", expectedIntValues, returnedIntValues);
    }

    @Test
    public void sortWithFilter() {
        Search search = search().filter(all()).sort(field("integer_1").reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithQuery() {
        Search search = search().query(all()).sort(field("integer_1").reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithFilterAndQuery() {
        Search search = search().filter(all()).query(all()).sort(field("integer_1").reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterNotReversed() {
        Search search = search().filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km"))
                                .sort(geoDistanceField("geo_point", -3.784519, 40.442163).reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceQueryNotReversed() {
        Search search = search().query(geoDistance("geo_point", -3.784519, 40.442163, "10000km"))
                                .sort(geoDistanceField("geo_point", -3.784519, 40.442163).reverse(false));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterReversed() {
        Search search = search().filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km"))
                                .sort(geoDistanceField("geo_point", -3.784519, 40.442163).reverse(true));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceQueryReversed() {
        Search search = search().query(geoDistance("geo_point", -3.784519, 40.442163, "10000km"))
                                .sort(geoDistanceField("geo_point", -3.784519, 40.442163).reverse(true));
        Integer[] returnedValues = intColumn(utils.searchWithPreparedStatement(search), "integer_1");
        Assert.assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }
}