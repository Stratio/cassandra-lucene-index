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

<<<<<<< HEAD
import com.stratio.cassandra.lucene.query.builder.RangeConditionBuilder;
import com.stratio.cassandra.lucene.query.builder.SearchBuilder;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import com.stratio.cassandra.lucene.schema.mapping.FloatMapper;
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.*;
=======
import com.stratio.cassandra.lucene.query.builder.GeoBBoxConditionBuilder;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.geobbox;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.query;
>>>>>>> Add tests and fix bugs for GeoBBoxCondition.
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoBBoxConditionTest extends AbstractConditionTest {

    @Test
<<<<<<< HEAD
    public void testStringClose() {

        Schema schema = mockSchema("name", new StringMapper("name", null, null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("beta", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringOpen() {

        Schema schema = mockSchema("name", new StringMapper("name", null, null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals(null, ((TermRangeQuery) query).getUpperTerm());
        assertNull(((TermRangeQuery) query).getUpperTerm());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(false, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerClose() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerOpen() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongClose() {

        Schema schema = mockSchema("name", new LongMapper("name", true, true, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43L, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongOpen() {

        Schema schema = mockSchema("name", new LongMapper("name", true, true, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatClose() {

        Schema schema = mockSchema("name", new FloatMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43.42f, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatOpen() {

        Schema schema = mockSchema("name", new FloatMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42f, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleClose() {

        Schema schema = mockSchema("name", new DoubleMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43.42D, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleOpen() {

        Schema schema = mockSchema("name", new DoubleMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "192.168.0.01", "192.168.0.045", true, true);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("192.168.0.1", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("192.168.0.45", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RangeCondition rangeCondition = range("name").boost(0.5f)
                                                     .lower("2001:DB8:2de::e13")
                                                     .upper("2001:DB8:02de::e23")
                                                     .includeLower(true)
                                                     .includeUpper(true)
                                                     .build();
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("2001:db8:2de:0:0:0:0:e23", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJsonInteger() {
        RangeConditionBuilder rangeCondition = range("name").lower(1)
                                                            .upper(2)
                                                            .includeLower(true)
                                                            .includeUpper(false)
                                                            .boost(0.5f);
        SearchBuilder searchBuilder = query(rangeCondition);

        testJsonCondition(searchBuilder);
    }

    @Test
    public void testJsonDouble() {
        testJsonCondition(query(range("name").lower(1.6)
                                             .upper(2.5)
                                             .includeLower(true)
                                             .includeUpper(false)
                                             .boost(0.5f)));
    }

    @Test
    public void testJsonString() {
        testJsonCondition(query(range("name").lower("a")
                                             .upper("b")
                                             .includeLower(true)
                                             .includeUpper(false)
                                             .boost(0.5f)).filter(bool().must(match("f1", "v1").boost(2),
                                                                              match("f2", "v2"))
                                                                        .should(match("f3", "v3"))
                                                                        .boost(0.5)).build());
=======
    public void testConstructor() {
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -180D, 180D, -90D, 90D);
        assertEquals(0.5, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(-180, condition.getMinLongitude(), 0);
        assertEquals(180, condition.getMaxLongitude(), 0);
        assertEquals(-90, condition.getMinLatitude(), 0);
        assertEquals(90, condition.getMaxLatitude(), 0);
    }

    @Test
    public void testConstructorWithDefaults() {
        GeoBBoxCondition condition = new GeoBBoxCondition(null, "name", 0D, 1D, 2D, 3D);
        assertEquals(GeoBBoxCondition.DEFAULT_BOOST, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(0, condition.getMinLongitude(), 0);
        assertEquals(1, condition.getMaxLongitude(), 0);
        assertEquals(2, condition.getMinLatitude(), 0);
        assertEquals(3, condition.getMaxLatitude(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullField() {
        new GeoBBoxCondition(null, null, 0D, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyField() {
        new GeoBBoxCondition(null, "", 0D, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithBlankField() {
        new GeoBBoxCondition(null, " ", 0D, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullMinLongitude() {
        new GeoBBoxCondition(null, "name", null, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithToSmallMinLongitude() {
        new GeoBBoxCondition(null, "name", -181D, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithToBiglMinLongitude() {
        new GeoBBoxCondition(null, "name", 181D, 1D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullMaxLongitude() {
        new GeoBBoxCondition(null, "name", 0D, null, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooSmallMaxLongitude() {
        new GeoBBoxCondition(null, "name", 0D, -181D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooBigMaxLongitude() {
        new GeoBBoxCondition(null, "name", 0D, 181D, 2D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, null, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooSmallMinLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, -91D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooBigMinLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, 91D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullMaxLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, 2D, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooSmallMaxLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, 2D, -91D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooBigMaxLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, 2D, 91D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithMinLongitudeGreaterThanMaxLongitude() {
        new GeoBBoxCondition(null, "name", 2D, 1D, 3D, 3D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithMinLatitudeGreaterThanMaxLatitude() {
        new GeoBBoxCondition(null, "name", 0D, 1D, 4D, 3D);
    }

    @Test
    public void testQuery() {
        Schema schema = mockSchema("name", new GeoPointMapper("name", "lon", "lat", 8));
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -180D, 180D, -90D, 90D);
        Query query = condition.query(schema);
        assertNotNull(query);
        assertTrue(query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue(query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter filter = (IntersectsPrefixTreeFilter) query;
        assertEquals("IntersectsPrefixTreeFilter(" +
                     "fieldName=name," +
                     "queryShape=Rect(minX=-180.0,maxX=180.0,minY=-90.0,maxY=90.0)," +
                     "detailLevel=3," +
                     "prefixGridScanLevel=4)", filter.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = mockSchema("name", new UUIDMapper("name", null, null));
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -180D, 180D, -90D, 90D);
        condition.query(schema);
    }

    @Test
    public void testJson() {
        GeoBBoxConditionBuilder condition = geobbox("name", -180D, 180D, -90D, 90D).boost(0.5f);
        testJsonCondition(query(condition));
>>>>>>> Add tests and fix bugs for GeoBBoxCondition.
    }

    @Test
    public void testToString() {
<<<<<<< HEAD
        RangeCondition condition = range("name").boost(0.5f)
                                                .lower("2001:DB8:2de::e13")
                                                .upper("2001:DB8:02de::e23")
                                                .includeLower(true)
                                                .includeUpper(true)
                                                .build();
        assertEquals("RangeCondition{boost=0.5, field=name, lower=2001:DB8:2de::e13, " +
                     "upper=2001:DB8:02de::e23, includeLower=true, includeUpper=true}", condition.toString());
=======
        GeoBBoxCondition condition = geobbox("name", -180D, 180D, -90D, 90D).boost(0.5f).build();
        assertEquals("GeoBBoxCondition{boost=0.5, field=name, " +
                     "minLongitude=-180.0, maxLongitude=180.0, minLatitude=-90.0, maxLatitude=90.0}",
                     condition.toString());
>>>>>>> Add tests and fix bugs for GeoBBoxCondition.
    }

}
