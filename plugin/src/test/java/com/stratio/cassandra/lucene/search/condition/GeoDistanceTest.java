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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoDistance;
import com.stratio.cassandra.lucene.common.GeoDistanceUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoDistanceTest extends AbstractConditionTest {

    @Test(expected = IndexException.class)
    public void testParseNull() {
        GeoDistance.parse(null);
    }

    @Test(expected = IndexException.class)
    public void testParseEmpty() {
        GeoDistance.parse("");
    }

    @Test(expected = IndexException.class)
    public void testParseBlank() {
        GeoDistance.parse("\t ");
    }

    @Test
    public void testParseMillimetres() {
        GeoDistance distance = GeoDistance.parse("4mm");
        check(distance, 0.004);
        distance = GeoDistance.parse("0.4millimetres");
        check(distance, 0.0004);
    }

    @Test
    public void testParseCentimetres() {
        GeoDistance distance = GeoDistance.parse("4cm");
        check(distance, 0.04);
        distance = GeoDistance.parse("0.4centimetres");
        check(distance, 0.004);
    }

    @Test
    public void testParseDecimetres() {
        GeoDistance distance = GeoDistance.parse("4dm");
        check(distance, 0.4);
        distance = GeoDistance.parse("0.4decimetres");
        check(distance, 0.04);
    }

    @Test
    public void testParseMetres() {
        GeoDistance distance = GeoDistance.parse("2m");
        check(distance, 2);
        distance = GeoDistance.parse("0.2metres");
        check(distance, 0.2);
    }

    @Test
    public void testParseDecametres() {
        GeoDistance distance = GeoDistance.parse("2dam");
        check(distance, 20);
        distance = GeoDistance.parse("0.2decametres");
        check(distance, 2);
    }

    @Test
    public void testParseHectometres() {
        GeoDistance distance = GeoDistance.parse("2hm");
        check(distance, 200);
        distance = GeoDistance.parse("0.2hectometres");
        check(distance, 20);
    }

    @Test
    public void testParseKilometres() {
        GeoDistance distance = GeoDistance.parse("2km");
        check(distance, 2000);
        distance = GeoDistance.parse("0.2kilometres");
        check(distance, 200);
    }

    @Test
    public void testParseFoots() {
        GeoDistance distance = GeoDistance.parse("2ft");
        check(distance, 0.6096);
        distance = GeoDistance.parse("0.2foots");
        check(distance, 0.06096);
    }

    @Test
    public void testParseYards() {
        GeoDistance distance = GeoDistance.parse("2yd");
        check(distance, 1.8288);
        distance = GeoDistance.parse("0.2yards");
        check(distance, 0.18288);
    }

    @Test
    public void testParseInches() {
        GeoDistance distance = GeoDistance.parse("2in");
        check(distance, 0.0508);
        distance = GeoDistance.parse("0.2inches");
        check(distance, 0.00508);
    }

    @Test
    public void testParseMiles() {
        GeoDistance distance = GeoDistance.parse("2mi");
        check(distance, 3218.688);
        distance = GeoDistance.parse("0.2miles");
        check(distance, 321.8688);
    }

    @Test
    public void testParseNauticalMiles() {
        GeoDistance distance = GeoDistance.parse("2M");
        check(distance, 3700);
        distance = GeoDistance.parse("0.2NM");
        check(distance, 370);
        distance = GeoDistance.parse("0.2mil");
        check(distance, 370);
        distance = GeoDistance.parse("0.2nautical_miles");
        check(distance, 370);
    }

    private void check(GeoDistance distance, double expected) {
        assertEquals("Parsed distance is wrong", expected, distance.getValue(GeoDistanceUnit.METRES), 0.0000000001);
    }

}
