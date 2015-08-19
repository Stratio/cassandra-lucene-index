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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.GeoBBoxConditionBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.geoBBox;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoBBoxConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        GeoBBoxConditionBuilder builder = new GeoBBoxConditionBuilder("name", -90D, 90D, -180D, 180D).boost(0.5f);
        GeoBBoxCondition condition = builder.build();
        assertEquals("Boost is not set", 0.5, condition.boost, 0);
        assertEquals("Field is not set", "name", condition.field);
        assertEquals("Min longitude is not set", -180, condition.minLongitude, 0);
        assertEquals("Max longitude is not set", 180, condition.maxLongitude, 0);
        assertEquals("Min latitude is not set", -90, condition.minLatitude, 0);
        assertEquals("Max latitude is not set", 90, condition.maxLatitude, 0);
    }

    @Test
    public void testBuildDefaults() {
        GeoBBoxConditionBuilder builder = new GeoBBoxConditionBuilder("name", 2D, 3D, 0D, 1D);
        GeoBBoxCondition condition = builder.build();
        assertEquals("Boost is not to default", GeoBBoxCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "name", condition.field);
        assertEquals("Min longitude is not set", 0, condition.minLongitude, 0);
        assertEquals("Max longitude is not set", 1, condition.maxLongitude, 0);
        assertEquals("Min latitude is not set", 2, condition.minLatitude, 0);
        assertEquals("Max latitude is not set", 3, condition.maxLatitude, 0);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new GeoBBoxCondition(null, null, 2D, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildEmptyField() {
        new GeoBBoxCondition(null, "", 2D, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildBlankField() {
        new GeoBBoxCondition(null, " ", 2D, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullMinLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, null, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildToSmallMinLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, -181D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooBiglMinLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, 181D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullMaxLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, 0D, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooSmallMaxLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, 0D, -181D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooBigMaxLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 3D, 0D, 181D);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullLatitude() {
        new GeoBBoxCondition(null, "name", null, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooSmallMinLatitude() {
        new GeoBBoxCondition(null, "name", -91D, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooBigMinLatitude() {
        new GeoBBoxCondition(null, "name", 91D, 3D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullMaxLatitude() {
        new GeoBBoxCondition(null, "name", 2D, null, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooSmallMaxLatitude() {
        new GeoBBoxCondition(null, "name", 2D, -91D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildTooBigMaxLatitude() {
        new GeoBBoxCondition(null, "name", 2D, 91D, 0D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildMinLongitudeGreaterThanMaxLongitude() {
        new GeoBBoxCondition(null, "name", 3D, 3D, 2D, 1D);
    }

    @Test(expected = IndexException.class)
    public void testBuildMinLatitudeGreaterThanMaxLatitude() {
        new GeoBBoxCondition(null, "name", 4D, 3D, 0D, 1D);
    }

    @Test
    public void testQuery() {
        Schema schema = schema().mapper("name", geoPointMapper("lat", "lon").maxLevels(8)).build();
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -90D, 90D, -180D, 180D);
        Query query = condition.query(schema);
        assertNotNull("Query is wrong is not built", query);
        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof BooleanQuery);
    }

    @Test(expected = IndexException.class)
    public void testQueryoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -90D, 90D, -180D, 180D);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        GeoBBoxCondition condition = geoBBox("name", -180D, 180D, -90D, 90D).boost(0.5f).build();
        assertEquals("Method #toString is wrong",
                     "GeoBBoxCondition{boost=0.5, field=name, " +
                     "minLatitude=-90.0, maxLatitude=90.0, minLongitude=-180.0, maxLongitude=180.0}",
                     condition.toString());
    }

}
