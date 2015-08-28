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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.geoDistance;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoDistanceConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructor() {
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, "3km", "10km");
        assertEquals("Boost is not set", 0.5, condition.boost, 0);
        assertEquals("Field is not set", "name", condition.field);
        assertEquals("Longitude is not set", -180, condition.longitude, 0);
        assertEquals("Latitude is not set", 90, condition.latitude, 0);
        assertEquals("Min distance is not set", "3km", condition.minDistance);
        assertEquals("Max distance is not set", "10km", condition.maxDistance);
    }

    @Test
    public void testConstructorWithDefaults() {
        GeoDistanceCondition condition = new GeoDistanceCondition(null, "name", 90D, -180D, null, "1yd");
        assertEquals("Boost is not to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "name", condition.field);
        assertEquals("Longitude is not set", -180, condition.longitude, 0);
        assertEquals("Latitude is not set", 90, condition.latitude, 0);
        assertNull("Min distance is not set", condition.minDistance);
        assertEquals("Max distance is not set", "1yd", condition.maxDistance);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullField() {
        new GeoDistanceCondition(null, null, 90D, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyField() {
        new GeoDistanceCondition(null, "", 90D, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankField() {
        new GeoDistanceCondition(null, " ", 90D, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLongitude() {
        new GeoDistanceCondition(null, "name", 90D, null, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithToSmallLongitude() {
        new GeoDistanceCondition(null, "name", 90D, -181D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithToBigLongitude() {
        new GeoDistanceCondition(null, "name", 90D, 181D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullLatitude() {
        new GeoDistanceCondition(null, "name", null, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithTooSmallLatitude() {
        new GeoDistanceCondition(null, "name", -91D, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithTooBigLatitude() {
        new GeoDistanceCondition(null, "name", 91D, -180D, "1km", "3km");
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithoutDistances() {
        new GeoDistanceCondition(null, "name", 90D, -180D, null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithMinLongitudeGreaterThanMaxLongitude() {
        new GeoDistanceCondition(null, "name", 90D, -180D, "10km", "3km");
    }

    @Test
    public void testQueryMax() {
        Schema schema = schema().mapper("name", geoPointMapper("lat", "lon").maxLevels(8)).build();
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, null, "10hm");
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals("Query num clauses is wrong", 1, booleanQuery.clauses().size());
        BooleanClause maxClause = booleanQuery.clauses().get(0);
        assertEquals("Query occur is wrong", BooleanClause.Occur.FILTER, maxClause.getOccur());
        query = maxClause.getQuery();
        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter filter = (IntersectsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeFilter(fieldName=name.dist,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 1.00km),detailLevel=8,prefixGridScanLevel=4)",
                     filter.toString());
    }

    @Test(expected = IndexException.class)
    public void testQueryMin() {
        new GeoDistanceCondition(0.5f, "name", 90D, -180D, "3km", null);
    }

    @Test
    public void testQueryMinMax() {
        Schema schema = schema().mapper("name", geoPointMapper("lat", "lon").maxLevels(8)).build();
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, "1km", "3km");
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertTrue("Query type is wrong", query instanceof BooleanQuery);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals("Query num clauses is wrong", 2, booleanQuery.clauses().size());

        BooleanClause minClause = booleanQuery.clauses().get(1);
        assertEquals("Query is wrong", BooleanClause.Occur.MUST_NOT, minClause.getOccur());
        query = minClause.getQuery();
        assertTrue("Query is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query is wrong", query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter minFilter = (IntersectsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeFilter(fieldName=name.dist,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 1.00km),detailLevel=8,prefixGridScanLevel=4)",
                     minFilter.toString());

        BooleanClause maxClause = booleanQuery.clauses().get(0);
        assertEquals("Query is wrong", BooleanClause.Occur.FILTER, maxClause.getOccur());
        query = maxClause.getQuery();
        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter maxFilter = (IntersectsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeFilter(fieldName=name.dist,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 3.00km),detailLevel=8,prefixGridScanLevel=4)",
                     maxFilter.toString());
    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        Condition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, null, "3km");
        condition.query(schema);
    }

    @Test
    public void testToString() {
        GeoDistanceCondition condition = geoDistance("name", -1D, 9, "3km").setMinDistance("1km").boost(0.4f).build();
        assertEquals("Method #toString is wrong",
                     "GeoDistanceCondition{boost=0.4, field=name, " +
                     "latitude=9.0, longitude=-1.0, minDistance=1km, maxDistance=3km}",
                     condition.toString());
    }

}
