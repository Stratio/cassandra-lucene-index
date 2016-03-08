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

package com.stratio.cassandra.lucene.testsAT.search;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SortedSearchAT extends AbstractSearchAT {

    @Test
    public void sortIntegerAsc() {
        Integer[] returnedValues = sort(field("integer_1").reverse(false)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDesc() {
        Integer[] returnedValues = sort(field("integer_1").reverse(true)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDefault() {
        Integer[] returnedValues = sort(field("integer_1")).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleAsc() {
        Double[] returnedValues = sort(field("double_1").reverse(false)).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDesc() {
        Double[] returnedValues = sort(field("double_1").reverse(true)).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{3D, 3D, 3D, 2D, 1D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDefault() {
        Double[] returnedValues = sort(field("double_1")).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortCombined() {
        Double[] returnedDoubleValues = sort(field("double_1"), field("integer_1")).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedDoubleValues.length);
        Integer[] returnedIntValues = sort(field("double_1"), field("integer_1")).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedIntValues.length);
        Double[] expectedDoubleValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        Integer[] expectedIntValues = new Integer[]{-1, -2, -5, -4, -3};
        assertArrayEquals("Wrong doubles sort!", expectedDoubleValues, returnedDoubleValues);
        assertArrayEquals("Wrong integers sort!", expectedIntValues, returnedIntValues);
    }

    @Test
    public void sortWithFilter() {
        Integer[] returnedValues = filter(all()).sort(field("integer_1").reverse(false)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithQuery() {
        Integer[] returnedValues = query(all()).sort(field("integer_1").reverse(false)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithFilterAndQuery() {
        Integer[] returnedValues = filter(all()).query(all())
                                                .sort(field("integer_1").reverse(false))
                                                .intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterNotReversed() {

        Integer[] returnedValues = filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceField("geo_point", -3.784519, 40.442163).reverse(false)).intColumn("integer_1");

        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);

    }

    @Test
    public void sortWithGeoDistanceQueryNotReversed() {
        Integer[] returnedValues = query(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceField("geo_point", -3.784519, 40.442163).reverse(false)).intColumn("integer_1");

        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceFilterReversed() {

        Integer[] returnedValues = filter(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceField("geo_point", -3.784519, 40.442163).reverse(true)).intColumn("integer_1");

        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortWithGeoDistanceQueryReversed() {

        Integer[] returnedValues = query(geoDistance("geo_point", -3.784519, 40.442163, "10000km")).sort(
                geoDistanceField("geo_point", -3.784519, 40.442163).reverse(true)).intColumn("integer_1");

        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong geoDistance sort!", expectedValues, returnedValues);

    }
}
