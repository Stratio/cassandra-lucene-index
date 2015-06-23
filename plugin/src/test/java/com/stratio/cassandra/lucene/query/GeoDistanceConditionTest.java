/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.geoDistance;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.query;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructor() {
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, "3km", "10km");
        assertEquals(0.5, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(-180, condition.longitude, 0);
        assertEquals(90, condition.latitude, 0);
        assertEquals("3km", condition.minDistance);
        assertEquals("10km", condition.maxDistance);
    }

    @Test
    public void testConstructorWithDefaults() {
        GeoDistanceCondition condition = new GeoDistanceCondition(null, "name", 90D, -180D, null, "1yd");
        assertEquals(GeoBBoxCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(-180, condition.longitude, 0);
        assertEquals(90, condition.latitude, 0);
        assertNull(condition.minDistance);
        assertEquals("1yd", condition.maxDistance);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullField() {
        new GeoDistanceCondition(null, null, 90D, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyField() {
        new GeoDistanceCondition(null, "", 90D, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankField() {
        new GeoDistanceCondition(null, " ", 90D, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullLongitude() {
        new GeoDistanceCondition(null, "name", 90D, null, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithToSmallLongitude() {
        new GeoDistanceCondition(null, "name", 90D, -181D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithToBigLongitude() {
        new GeoDistanceCondition(null, "name", 90D, 181D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullLatitude() {
        new GeoDistanceCondition(null, "name", null, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooSmallLatitude() {
        new GeoDistanceCondition(null, "name", -91D, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooBigLatitude() {
        new GeoDistanceCondition(null, "name", 91D, -180D, "1km", "3km");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithoutDistances() {
        new GeoDistanceCondition(null, "name", 90D, -180D, null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithMinLongitudeGreaterThanMaxLongitude() {
        new GeoDistanceCondition(null, "name", 90D, -180D, "10km", "3km");
    }

    @Test
    public void testQueryMax() {
        Schema schema = mockSchema("name", new GeoPointMapper("name", "lat", "lon", 8));
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, null, "10hm");
        Query query = condition.query(schema);
        assertNotNull(query);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals(1, booleanQuery.getClauses().length);
        BooleanClause maxClause = booleanQuery.getClauses()[0];
        assertEquals(BooleanClause.Occur.MUST, maxClause.getOccur());
        query = maxClause.getQuery();
        assertTrue(query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue(query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter filter = (IntersectsPrefixTreeFilter) query;
        assertEquals("IntersectsPrefixTreeFilter(fieldName=name,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 1.00km),detailLevel=8,prefixGridScanLevel=4)", filter.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryMin() {
        new GeoDistanceCondition(0.5f, "name", 90D, -180D, "3km", null);
    }

    @Test
    public void testQueryMinMax() {
        Schema schema = mockSchema("name", new GeoPointMapper("name", "lat", "lon", 8));
        GeoDistanceCondition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, "1km", "3km");
        Query query = condition.query(schema);
        assertNotNull(query);
        assertTrue(query instanceof BooleanQuery);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals(2, booleanQuery.getClauses().length);

        BooleanClause minClause = booleanQuery.getClauses()[1];
        assertEquals(BooleanClause.Occur.MUST_NOT, minClause.getOccur());
        query = minClause.getQuery();
        assertTrue(query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue(query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter minFilter = (IntersectsPrefixTreeFilter) query;
        assertEquals("IntersectsPrefixTreeFilter(fieldName=name,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 1.00km),detailLevel=8,prefixGridScanLevel=4)", minFilter.toString());

        BooleanClause maxClause = booleanQuery.getClauses()[0];
        assertEquals(BooleanClause.Occur.MUST, maxClause.getOccur());
        query = maxClause.getQuery();
        assertTrue(query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue(query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter maxFilter = (IntersectsPrefixTreeFilter) query;
        assertEquals("IntersectsPrefixTreeFilter(fieldName=name,queryShape=Circle(Pt(x=-180.0,y=90.0), " +
                     "d=0.0° 3.00km),detailLevel=8,prefixGridScanLevel=4)", maxFilter.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = mockSchema("name", new UUIDMapper("name", null, null));
        Condition condition = new GeoDistanceCondition(0.5f, "name", 90D, -180D, null, "3km");
        condition.query(schema);
    }

    @Test
    public void testJson() {
        testJsonCondition(query(geoDistance("name", -180D, 90D).setMinDistance("1km").setMaxDistance("3km")));
    }

    @Test
    public void testToString() {
        GeoDistanceCondition condition = geoDistance("name", -1D, 9).setMinDistance("1km")
                                                                    .setMaxDistance("3km")
                                                                    .boost(0.4f)
                                                                    .build();
        assertEquals("GeoDistanceCondition{field=name, latitude=9.0, longitude=-1.0, minDistance=1km, maxDistance=3km}",
                     condition.toString());
    }

}
