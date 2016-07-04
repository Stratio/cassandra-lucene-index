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
package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.testsAT.search.AbstractSearchAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class BoundStatementWithSortedKQuery extends AbstractSearchAT {

    @Test
    public void sortIntegerAsc() {
        utils.sort(field("integer_1").reverse(false)).checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortIntegerDesc() {
        utils.sort(field("integer_1").reverse(true)).checkOrderedColumns("integer_1", -1, -2, -3, -4, -5);
    }

    @Test
    public void sortIntegerDefault() {
        utils.sort(field("integer_1")).checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortDoubleAsc() {
        utils.sort(field("double_1").reverse(false)).checkOrderedColumns("double_1", 1D, 2D, 3D, 3D, 3D);
    }

    @Test
    public void sortDoubleDesc() {
        utils.sort(field("double_1").reverse(true)).checkOrderedColumns("double_1", 3D, 3D, 3D, 2D, 1D);
    }

    @Test
    public void sortDoubleDefault() {
        utils.sort(field("double_1")).checkOrderedColumns("double_1", 1D, 2D, 3D, 3D, 3D);
    }

    @Test
    public void sortCombined() {
        CassandraUtilsSelect select = utils.sort(field("double_1"), field("integer_1"));
        select.checkOrderedColumns("double_1", 1D, 2D, 3D, 3D, 3D);
        select.checkOrderedColumns("integer_1", -1, -2, -5, -4, -3);
    }

    @Test
    public void sortWithFilter() {
        utils.filter(all())
             .sort(field("integer_1").reverse(false))
             .checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortWithQuery() {
        utils.query(all())
             .sort(field("integer_1").reverse(false))
             .checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortWithFilterAndQuery() {
        utils.filter(all())
             .query(all())
             .sort(field("integer_1").reverse(false))
             .checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortWithGeoDistanceFilterNotReversed() {
        utils.filter(geoDistance("geo_point", 40.442163, -3.784519, "10000km"))
             .sort(geoDistanceField("geo_point", 40.442163, -3.784519).reverse(false))
             .checkOrderedColumns("integer_1", -1, -2, -3, -4, -5);
    }

    @Test
    public void sortWithGeoDistanceQueryNotReversed() {
        utils.query(geoDistance("geo_point", 40.442163, -3.784519, "10000km"))
             .sort(geoDistanceField("geo_point", 40.442163, -3.784519).reverse(false))
             .checkOrderedColumns("integer_1", -1, -2, -3, -4, -5);
    }

    @Test
    public void sortWithGeoDistanceFilterReversed() {
        utils.filter(geoDistance("geo_point", 40.442163, -3.784519, "10000km"))
             .sort(geoDistanceField("geo_point", 40.442163, -3.784519).reverse(true))
             .checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }

    @Test
    public void sortWithGeoDistanceQueryReversed() {
        utils.query(geoDistance("geo_point", 40.442163, -3.784519, "10000km"))
             .sort(geoDistanceField("geo_point", 40.442163, -3.784519).reverse(true))
             .checkOrderedColumns("integer_1", -5, -4, -3, -2, -1);
    }
}