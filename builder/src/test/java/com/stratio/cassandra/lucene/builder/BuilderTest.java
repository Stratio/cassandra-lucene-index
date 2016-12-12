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
package com.stratio.cassandra.lucene.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.cassandra.lucene.builder.common.GeoShape;
import org.junit.Test;

import java.util.Arrays;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Builder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BuilderTest {

    @Test
    public void testIndexDefaults() {
        String actual = index("table", "idx").schema(schema()).build();
        String expected = "CREATE CUSTOM INDEX idx ON table() " +
                          "USING 'com.stratio.cassandra.lucene.Index' " +
                          "WITH OPTIONS = {'schema':'{}'}";
        assertEquals("index serialization is wrong", expected, actual);
    }

    @Test
    public void testIndexFull() {
        String actual = index("ks", "table", "idx").keyspace("keyspace")
                                                   .column("lucene")
                                                   .directoryPath("path")
                                                   .refreshSeconds(10D)
                                                   .maxCachedMb(32)
                                                   .maxMergeMb(16)
                                                   .ramBufferMb(64)
                                                   .indexingThreads(4)
                                                   .indexingQueuesSize(100)
                                                   .excludedDataCenters("DC1,DC2")
                                                   .partitioner(partitionerOnToken(8))
                                                   .defaultAnalyzer("my_analyzer")
                                                   .analyzer("my_analyzer", classpathAnalyzer("my_class"))
                                                   .analyzer("snow", snowballAnalyzer("tartar").stopwords("a,b,c"))
                                                   .mapper("uuid", uuidMapper().validated(true))
                                                   .mapper("string", stringMapper())
                                                   .build();
        String expected = "CREATE CUSTOM INDEX idx ON keyspace.table(lucene) " +
                          "USING 'com.stratio.cassandra.lucene.Index' " +
                          "WITH OPTIONS = {" +
                          "'refresh_seconds':'10.0'," +
                          "'directory_path':'path'," +
                          "'ram_buffer_mb':'64'," +
                          "'max_merge_mb':'16'," +
                          "'max_cached_mb':'32'," +
                          "'indexing_threads':'4'," +
                          "'indexing_queues_size':'100'," +
                          "'excluded_data_centers':'DC1,DC2'," +
                          "'partitioner':'{\"type\":\"token\",\"partitions\":8}'," +
                          "'schema':'{" +
                          "\"default_analyzer\":\"my_analyzer\",\"analyzers\":{" +
                          "\"my_analyzer\":{\"type\":\"classpath\",\"class\":\"my_class\"}," +
                          "\"snow\":{\"type\":\"snowball\",\"language\":\"tartar\",\"stopwords\":\"a,b,c\"}}," +
                          "\"fields\":{" +
                          "\"uuid\":{\"type\":\"uuid\",\"validated\":true},\"string\":{\"type\":\"string\"}}}'}";
        assertEquals("index serialization is wrong", expected, actual);
    }

    @Test
    public void testNonePartitioner() {
        String actual = nonePartitioner().build();
        String expected = "{\"type\":\"none\"}";
        assertEquals("none partitioner serialization is wrong", expected, actual);
    }

    @Test
    public void testTokenPartitioner() {
        String actual = partitionerOnToken(6).build();
        String expected = "{\"type\":\"token\",\"partitions\":6}";
        assertEquals("token partitioner serialization is wrong", expected, actual);
    }

    @Test
    public void testColumnPartitioner() {
        String actual = partitionerOnColumn(6, "col").build();
        String expected = "{\"type\":\"column\",\"partitions\":6,\"column\":\"col\"}";
        assertEquals("column partitioner serialization is wrong", expected, actual);
    }

    @Test
    public void testBigDecimalMapperDefaults() {
        String actual = bigDecimalMapper().build();
        String expected = "{\"type\":\"bigdec\"}";
        assertEquals("big decimal mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBigDecimalMapperFull() {
        String actual = bigDecimalMapper().validated(true).column("column").integerDigits(2).decimalDigits(1).build();
        String expected = "{\"type\":\"bigdec\"," +
                          "\"validated\":true," +
                          "\"column\":\"column\"," +
                          "\"integer_digits\":2," +
                          "\"decimal_digits\":1}";
        assertEquals("big decimal mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBigIntegerMapperDefaults() {
        String actual = bigIntegerMapper().build();
        String expected = "{\"type\":\"bigint\"}";
        assertEquals("big integer mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBigIntegerMapperFull() {
        String actual = bigIntegerMapper().validated(true).digits(1).column("column").build();
        String expected = "{\"type\":\"bigint\",\"validated\":true,\"column\":\"column\",\"digits\":1}";
        assertEquals("big integer mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBitemporalMapperDefaults() {
        String actual = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").build();
        String expected = "{\"type\":\"bitemporal\",\"vt_from\":\"vt_from\",\"vt_to\":\"vt_to\"" +
                          ",\"tt_from\":\"tt_from\",\"tt_to\":\"tt_to\"}";
        assertEquals("bitemporal mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBitemporalMapperFull() {
        String actual = bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").validated(true)
                                                                                .pattern("yyyyMMdd")
                                                                                .nowValue("99999999")
                                                                                .build();
        String expected = "{\"type\":\"bitemporal\"," +
                          "\"vt_from\":\"vt_from\"," +
                          "\"vt_to\":\"vt_to\"," +
                          "\"tt_from\":\"tt_from\"," +
                          "\"tt_to\":\"tt_to\"," +
                          "\"validated\":true," +
                          "\"pattern\":\"yyyyMMdd\"," +
                          "\"now_value\":\"99999999\"}";
        assertEquals("bitemporal mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBlobMapperDefaults() {
        String actual = blobMapper().build();
        String expected = "{\"type\":\"bytes\"}";
        assertEquals("blob condition serialization is wrong", expected, actual);
    }

    @Test
    public void testBlobMapperFull() {
        String actual = blobMapper().validated(true).column("column").build();
        String expected = "{\"type\":\"bytes\",\"validated\":true,\"column\":\"column\"}";
        assertEquals("blob mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBooleanMapperDefaults() {
        String actual = booleanMapper().build();
        String expected = "{\"type\":\"boolean\"}";
        assertEquals("boolean mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testBooleanMapperFull() {
        String actual = booleanMapper().validated(true).column("column").build();
        String expected = "{\"type\":\"boolean\",\"validated\":true,\"column\":\"column\"}";
        assertEquals("boolean mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDateMapperDefaults() {
        String actual = dateMapper().build();
        String expected = "{\"type\":\"date\"}";
        assertEquals("date mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDateMapperFull() {
        String actual = dateMapper().validated(true).column("column").pattern("yyyyMMdd").build();
        String expected = "{\"type\":\"date\"," +
                          "\"validated\":true," +
                          "\"column\":\"column\"," +
                          "\"pattern\":\"yyyyMMdd\"}";
        assertEquals("date mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDateRangeDefaults() {
        String actual = dateRangeMapper("start", "stop").build();
        String expected = "{\"type\":\"date_range\",\"from\":\"start\",\"to\":\"stop\"}";
        assertEquals("date range mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDateRangeMapperFull() {
        String actual = dateRangeMapper("start", "stop").pattern("yyyyMMdd").build();
        String expected = "{\"type\":\"date_range\"," +
                          "\"from\":\"start\"," +
                          "\"to\":\"stop\"," +
                          "\"pattern\":\"yyyyMMdd\"}";
        assertEquals("date range mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDoubleMapperDefaults() {
        String actual = doubleMapper().build();
        String expected = "{\"type\":\"double\"}";
        assertEquals("double mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testDoubleMapperFull() {
        String actual = doubleMapper().validated(true).boost(2.1f).column("column").build();
        String expected = "{\"type\":\"double\",\"validated\":true,\"column\":\"column\",\"boost\":2.1}";
        assertEquals("double mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testFloatMapperDefaults() {
        String actual = floatMapper().build();
        String expected = "{\"type\":\"float\"}";
        assertEquals("float mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testFloatMapperFull() {
        String actual = floatMapper().validated(true).boost(2.1f).column("column").build();
        String expected = "{\"type\":\"float\",\"validated\":true,\"column\":\"column\",\"boost\":2.1}";
        assertEquals("float mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoPointMapperDefaults() {
        String actual = geoPointMapper("lat", "lon").build();
        String expected = "{\"type\":\"geo_point\",\"latitude\":\"lat\",\"longitude\":\"lon\"}";
        assertEquals("geo point mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoPointMapperFull() {
        String actual = geoPointMapper("lat", "lon").maxLevels(7).build();
        String expected = "{\"type\":\"geo_point\",\"latitude\":\"lat\",\"longitude\":\"lon\",\"max_levels\":7}";
        assertEquals("geo point mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoShapeMapperDefaults() {
        String actual = geoShapeMapper().build();
        String expected = "{\"type\":\"geo_shape\"}";
        assertEquals("geo shape mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoShapeMapperFull() {
        String actual = geoShapeMapper().column("shape")
                                        .maxLevels(7)
                                        .transform(centroid(),
                                                   convexHull(),
                                                   bbox(),
                                                   buffer().maxDistance("10km").minDistance("5km"))
                                        .build();
        String expected = "{\"type\":\"geo_shape\",\"column\":\"shape\",\"max_levels\":7,\"transformations\":" +
                          "[{\"type\":\"centroid\"}," +
                          "{\"type\":\"convex_hull\"}," +
                          "{\"type\":\"bbox\"}," +
                          "{\"type\":\"buffer\",\"max_distance\":\"10km\",\"min_distance\":\"5km\"}]}";
        assertEquals("geo shape mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testInetMapperDefaults() {
        String actual = inetMapper().build();
        String expected = "{\"type\":\"inet\"}";
        assertEquals("inet mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testInetMapperFull() {
        String actual = inetMapper().validated(true).build();
        String expected = "{\"type\":\"inet\",\"validated\":true}";
        assertEquals("inet mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testIntegerMapperDefaults() {
        String actual = integerMapper().build();
        String expected = "{\"type\":\"integer\"}";
        assertEquals("integer mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testIntegerMapperFull() {
        String actual = integerMapper().validated(true).boost(2.1f).build();
        String expected = "{\"type\":\"integer\",\"validated\":true,\"boost\":2.1}";
        assertEquals("integer mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testLongMapperDefaults() {
        String actual = longMapper().build();
        String expected = "{\"type\":\"long\"}";
        assertEquals("long mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testLongMapperFull() {
        String actual = longMapper().validated(true).boost(2.1f).build();
        String expected = "{\"type\":\"long\",\"validated\":true,\"boost\":2.1}";
        assertEquals("long mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testStringMapperDefaults() {
        String actual = stringMapper().build();
        String expected = "{\"type\":\"string\"}";
        assertEquals("string mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testStringMapperFull() {
        String actual = stringMapper().validated(true).caseSensitive(true).build();
        String expected = "{\"type\":\"string\",\"validated\":true,\"case_sensitive\":true}";
        assertEquals("string mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testTextMapperDefaults() {
        String actual = textMapper().build();
        String expected = "{\"type\":\"text\"}";
        assertEquals("text mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testTextMapperFull() {
        String actual = textMapper().validated(true).analyzer("analyzer").build();
        String expected = "{\"type\":\"text\",\"validated\":true,\"analyzer\":\"analyzer\"}";
        assertEquals("text mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testUUIDMapperDefaults() {
        String actual = uuidMapper().build();
        String expected = "{\"type\":\"uuid\"}";
        assertEquals("UUID mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testUUIDMapperFull() {
        String actual = uuidMapper().validated(true).build();
        String expected = "{\"type\":\"uuid\",\"validated\":true}";
        assertEquals("UUID mapper serialization is wrong", expected, actual);
    }

    @Test
    public void testClasspathAnalyzer() {
        String actual = classpathAnalyzer("com.test.MyAnalyzer").build();
        String expected = "{\"type\":\"classpath\",\"class\":\"com.test.MyAnalyzer\"}";
        assertEquals("classpath analyzer serialization is wrong", expected, actual);
    }

    @Test
    public void testSnowballAnalyzerDefaults() {
        String actual = snowballAnalyzer("tartarus").build();
        String expected = "{\"type\":\"snowball\",\"language\":\"tartarus\"}";
        assertEquals("snowball analyzer serialization is wrong", expected, actual);
    }

    @Test
    public void testSnowballAnalyzerFull() {
        String actual = snowballAnalyzer("tartarus").stopwords("a,b,c").build();
        String expected = "{\"type\":\"snowball\",\"language\":\"tartarus\",\"stopwords\":\"a,b,c\"}";
        assertEquals("snowball analyzer serialization is wrong", expected, actual);
    }

    @Test
    public void testAllConditionDefaults() {
        String actual = all().build();
        String expected = "{\"type\":\"all\"}";
        assertEquals("all condition serialization is wrong", expected, actual);
    }

    @Test
    public void testAllConditionFull() {
        String actual = all().boost(2).build();
        String expected = "{\"type\":\"all\",\"boost\":2.0}";
        assertEquals("all condition serialization is wrong", expected, actual);
    }

    @Test
    public void testBitemporalConditionDefaults() {
        String actual = bitemporal("field").build();
        String expected = "{\"type\":\"bitemporal\",\"field\":\"field\"}";
        assertEquals("bitemporal condition serialization is wrong", expected, actual);
    }

    @Test
    public void testBitemporalConditionFull() {
        String actual = bitemporal("field").ttFrom(1).ttTo(2).vtFrom(3).vtTo(4).boost(2).build();
        String expected = "{\"type\":\"bitemporal\"," +
                          "\"field\":\"field\"," +
                          "\"boost\":2.0," +
                          "\"vt_from\":3," +
                          "\"vt_to\":4," +
                          "\"tt_from\":1," +
                          "\"tt_to\":2}";
        assertEquals("bitemporal condition serialization is wrong", expected, actual);
    }

    @Test
    public void testDateRangeConditionDefaults() {
        String actual = dateRange("field").build();
        String expected = "{\"type\":\"date_range\",\"field\":\"field\"}";
        assertEquals("date range condition serialization is wrong", expected, actual);
    }

    @Test
    public void testDateRangeConditionFull() {
        String actual = dateRange("field").from("2015/01/02").to("2015/01/05").operation("is_within").boost(2).build();
        String expected = "{\"type\":\"date_range\"," +
                          "\"field\":\"field\"," +
                          "\"boost\":2.0," +
                          "\"from\":\"2015/01/02\"," +
                          "\"to\":\"2015/01/05\"," +
                          "\"operation\":\"is_within\"}";
        assertEquals("date range condition serialization is wrong", expected, actual);
    }

    @Test
    public void testContainsConditionDefaults() {
        String actual = contains("field").build();
        String expected = "{\"type\":\"contains\",\"field\":\"field\",\"values\":[]}";
        assertEquals("contains condition serialization is wrong", expected, actual);
    }

    @Test
    public void testContainsConditionFull() {
        String actual = contains("field", "v1", "v2").boost(2).docValues(true).build();
        String expected = "{\"type\":\"contains\"," +
                          "\"field\":\"field\"," +
                          "\"values\":[\"v1\",\"v2\"]," +
                          "\"boost\":2.0," +
                          "\"doc_values\":true}";
        assertEquals("contains condition serialization is wrong", expected, actual);
    }

    @Test
    public void testBooleanConditionDefaults() {
        String actual = bool().build();
        String expected = "{\"type\":\"boolean\"}";
        assertEquals("boolean is wrong", expected, actual);
    }

    @Test
    public void testBooleanConditionFull() {
        String actual = bool().must(match("f1", 1), match("f2", 2))
                              .should(match("f3", 3), match("f4", 4))
                              .not(match("f5", 5), match("f6", 6))
                              .boost(2)
                              .build();
        String expected = "{\"type\":\"boolean\",\"boost\":2.0," +
                          "\"must\":[" +
                          "{\"type\":\"match\",\"field\":\"f1\",\"value\":1}," +
                          "{\"type\":\"match\",\"field\":\"f2\",\"value\":2}" +
                          "],\"should\":[" +
                          "{\"type\":\"match\",\"field\":\"f3\",\"value\":3}," +
                          "{\"type\":\"match\",\"field\":\"f4\",\"value\":4}" +
                          "],\"not\":[" +
                          "{\"type\":\"match\",\"field\":\"f5\",\"value\":5}," +
                          "{\"type\":\"match\",\"field\":\"f6\",\"value\":6}" +
                          "]}";
        assertEquals("boolean is wrong", expected, actual);
    }

    @Test
    public void testFuzzyConditionDefaults() {
        String actual = fuzzy("field", "value").build();
        String expected = "{\"type\":\"fuzzy\",\"field\":\"field\",\"value\":\"value\"}";
        assertEquals("fuzzy condition serialization is wrong", expected, actual);
    }

    @Test
    public void testFuzzyConditionFull() {
        String actual = fuzzy("field", "value").maxEdits(1)
                                               .maxExpansions(2)
                                               .prefixLength(3)
                                               .transpositions(true)
                                               .boost(2)
                                               .build();
        String expected = "{\"type\":\"fuzzy\",\"field\":\"field\",\"value\":\"value\",\"boost\":2.0," +
                          "\"max_edits\":1,\"prefix_length\":3,\"max_expansions\":2,\"transpositions\":true}";
        assertEquals("fuzzy condition serialization is wrong", expected, actual);
    }

    @Test
    public void testLuceneConditionDefaults() {
        String actual = lucene("").build();
        String expected = "{\"type\":\"lucene\",\"query\":\"\"}";
        assertEquals("lucene condition serialization is wrong", expected, actual);
    }

    @Test
    public void testLuceneConditionFull() {
        String actual = lucene("field:value").defaultField("field").boost(2).build();
        String expected = "{\"type\":\"lucene\",\"query\":\"field:value\",\"boost\":2.0,\"default_field\":\"field\"}";
        assertEquals("lucene condition serialization is wrong", expected, actual);
    }

    @Test
    public void testMatchConditionDefaults() {
        String actual = match("field", "value").build();
        String expected = "{\"type\":\"match\",\"field\":\"field\",\"value\":\"value\"}";
        assertEquals("match condition serialization is wrong", expected, actual);
    }

    @Test
    public void testMatchConditionFull() {
        String actual = match("field", "value").docValues(true).boost(2).build();
        String expected = "{\"type\":\"match\"," +
                          "\"field\":\"field\"," +
                          "\"value\":\"value\"," +
                          "\"boost\":2.0," +
                          "\"doc_values\":true}";
        assertEquals("match condition serialization is wrong", expected, actual);
    }

    @Test
    public void testNoneConditionDefaults() {
        String actual = none().build();
        String expected = "{\"type\":\"none\"}";
        assertEquals("none condition serialization is wrong", expected, actual);
    }

    @Test
    public void testNoneConditionFull() {
        String actual = none().boost(2).build();
        String expected = "{\"type\":\"none\",\"boost\":2.0}";
        assertEquals("none condition serialization is wrong", expected, actual);
    }

    @Test
    public void testPhraseConditionDefaults() {
        String actual = phrase("field", "value").build();
        String expected = "{\"type\":\"phrase\",\"field\":\"field\",\"value\":\"value\"}";
        assertEquals("phrase condition serialization is wrong", expected, actual);
    }

    @Test
    public void testPhraseConditionFull() {
        String actual = phrase("field", "value").slop(2).boost(2).build();
        String expected = "{\"type\":\"phrase\",\"field\":\"field\",\"value\":\"value\",\"boost\":2.0,\"slop\":2}";
        assertEquals("phrase condition serialization is wrong", expected, actual);
    }

    @Test
    public void testPrefixConditionDefaults() {
        String actual = prefix("field", "value").build();
        String expected = "{\"type\":\"prefix\",\"field\":\"field\",\"value\":\"value\"}";
        assertEquals("prefix condition serialization is wrong", expected, actual);
    }

    @Test
    public void testPrefixConditionFull() {
        String actual = prefix("field", "value").boost(1.5).build();
        String expected = "{\"type\":\"prefix\",\"field\":\"field\",\"value\":\"value\",\"boost\":1.5}";
        assertEquals("prefix condition serialization is wrong", expected, actual);
    }

    @Test
    public void testRangeConditionDefaults() {
        String actual = range("field").build();
        String expected = "{\"type\":\"range\",\"field\":\"field\"}";
        assertEquals("range condition serialization is wrong", expected, actual);
    }

    @Test
    public void testRangeConditionFull() {
        String actual = range("field").lower(1)
                                      .upper(2)
                                      .includeLower(true)
                                      .includeUpper(false)
                                      .docValues(true)
                                      .boost(0.3)
                                      .build();
        String expected = "{\"type\":\"range\"," +
                          "\"field\":\"field\"," +
                          "\"boost\":0.3," +
                          "\"lower\":1," +
                          "\"upper\":2," +
                          "\"include_lower\":true," +
                          "\"include_upper\":false," +
                          "\"doc_values\":true}";
        assertEquals("range condition serialization is wrong", expected, actual);
    }

    @Test
    public void testRegexpConditionDefaults() {
        String actual = regexp("field", "expression").build();
        String expected = "{\"type\":\"regexp\",\"field\":\"field\",\"value\":\"expression\"}";
        assertEquals("regexp condition serialization is wrong", expected, actual);
    }

    @Test
    public void testRegexpConditionFull() {
        String actual = regexp("field", "expression").boost(null).build();
        String expected = "{\"type\":\"regexp\",\"field\":\"field\",\"value\":\"expression\"}";
        assertEquals("regexp condition serialization is wrong", expected, actual);
    }

    @Test
    public void testWildcardConditionDefaults() {
        String actual = wildcard("field", "value").build();
        String expected = "{\"type\":\"wildcard\",\"field\":\"field\",\"value\":\"value\"}";
        assertEquals("wildcard condition serialization is wrong", expected, actual);
    }

    @Test
    public void testWildcardConditionFull() {
        String actual = wildcard("field", "value").boost(1.7).build();
        String expected = "{\"type\":\"wildcard\",\"field\":\"field\",\"value\":\"value\",\"boost\":1.7}";
        assertEquals("wildcard condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoBBoxConditionDefaults() {
        String actual = geoBBox("field", 1, 2, 3, 4).build();
        String expected = "{\"type\":\"geo_bbox\"," +
                          "\"field\":\"field\"," +
                          "\"min_latitude\":1.0," +
                          "\"max_latitude\":2.0," +
                          "\"min_longitude\":3.0," +
                          "\"max_longitude\":4.0}";
        assertEquals("wildcard condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoBBoConditionFull() {
        String actual = geoBBox("field", 1, 2, 3, 4).boost(1).build();
        String expected = "{\"type\":\"geo_bbox\"," +
                          "\"field\":\"field\"," +
                          "\"min_latitude\":1.0," +
                          "\"max_latitude\":2.0," +
                          "\"min_longitude\":3.0," +
                          "\"max_longitude\":4.0," +
                          "\"boost\":1.0}";
        assertEquals("wildcard condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoDistanceConditionDefaults() {
        String actual = geoDistance("field", 2, 1, "1km").build();
        String expected = "{\"type\":\"geo_distance\"," +
                          "\"field\":\"field\"," +
                          "\"latitude\":2.0," +
                          "\"longitude\":1.0," +
                          "\"max_distance\":\"1km\"}";
        assertEquals("geo distance condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoDistanceConditionFull() {
        String actual = geoDistance("field", 2, 1, "1km").minDistance("500m").boost(0.5).build();
        String expected = "{\"type\":\"geo_distance\"," +
                          "\"field\":\"field\"," +
                          "\"latitude\":2.0," +
                          "\"longitude\":1.0," +
                          "\"max_distance\":\"1km\"," +
                          "\"boost\":0.5," +
                          "\"min_distance\":\"500m\"}";
        assertEquals("geo distance condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoShapeBuffer() {
        String expected = "{\"type\":\"buffer\",\"shape\":{\"type\":\"wkt\",\"value\":\"1\"}," +
                          "\"max_distance\":\"1\",\"min_distance\":\"2\"}";
        assertEquals("Shape buffer is wrong", expected, buffer(wkt("1")).maxDistance("1").minDistance("2").build());
        assertEquals("Shape buffer is wrong", expected, buffer("1").maxDistance("1").minDistance("2").build());
    }

    @Test
    public void testGeoShapeBBox() {
        String expected = "{\"type\":\"bbox\",\"shape\":{\"type\":\"wkt\",\"value\":\"1\"}}";
        assertEquals("Shape bbox is wrong", expected, bbox(wkt("1")).build());
        assertEquals("Shape bbox is wrong", expected, bbox("1").build());
    }

    @Test
    public void testGeoShapeCentroid() {
        String expected = "{\"type\":\"centroid\",\"shape\":{\"type\":\"wkt\",\"value\":\"1\"}}";
        assertEquals("Shape centroid is wrong", expected, centroid(wkt("1")).build());
        assertEquals("Shape centroid is wrong", expected, centroid("1").build());
    }

    @Test
    public void testGeoShapeConvexHull() {
        String expected = "{\"type\":\"convex_hull\",\"shape\":{\"type\":\"wkt\",\"value\":\"1\"}}";
        assertEquals("Shape convex hull is wrong", expected, convexHull(wkt("1")).build());
        assertEquals("Shape convex hull is wrong", expected, convexHull("1").build());
    }

    @Test
    public void testGeoShapeIntersection() {
        String expected = "{\"type\":\"intersection\",\"shapes\":[" +
                          "{\"type\":\"wkt\",\"value\":\"1\"}," +
                          "{\"type\":\"wkt\",\"value\":\"2\"}" +
                          "]}";
        assertEquals("Shape intersection is wrong", expected, intersection(wkt("1"),wkt("2")).build());
        assertEquals("Shape intersection is wrong", expected, intersection("1","2").build());
        assertEquals("Shape intersection is wrong", expected, intersection(Arrays.asList(wkt("1"),wkt("2"))).build());
        assertEquals("Shape intersection is wrong", expected, intersection().add("1").add(wkt("2")).build());
    }

    @Test
    public void testGeoShapeUnion() {
        String expected = "{\"type\":\"union\",\"shapes\":[" +
                          "{\"type\":\"wkt\",\"value\":\"1\"}," +
                          "{\"type\":\"wkt\",\"value\":\"2\"}" +
                          "]}";
        assertEquals("Shape union is wrong", expected, union(wkt("1"),wkt("2")).build());
        assertEquals("Shape union is wrong", expected, union("1","2").build());
        assertEquals("Shape union is wrong", expected, union(Arrays.asList(wkt("1"),wkt("2"))).build());
        assertEquals("Shape union is wrong", expected, union().add("1").add(wkt("2")).build());
    }

    @Test
    public void testGeoShapeDifference() {
        String expected = "{\"type\":\"difference\",\"shapes\":[" +
                          "{\"type\":\"wkt\",\"value\":\"1\"}," +
                          "{\"type\":\"wkt\",\"value\":\"2\"}" +
                          "]}";
        assertEquals("Shape difference is wrong", expected, difference(wkt("1"),wkt("2")).build());
        assertEquals("Shape difference is wrong", expected, difference("1","2").build());
        assertEquals("Shape difference is wrong", expected, difference(Arrays.asList(wkt("1"),wkt("2"))).build());
        assertEquals("Shape difference is wrong", expected, difference().add("1").add(wkt("2")).build());
    }

    @Test
    public void testGeoShapeConditionDefaults() {
        String actual = geoShape("field", wkt("POINT(0 0)")).build();
        String expected = "{\"type\":\"geo_shape\",\"field\":\"field\"," +
                          "\"shape\":{\"type\":\"wkt\",\"value\":\"POINT(0 0)\"}}";
        assertEquals("geo shape condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoShapeConditionFull() {
        GeoShape shape = union(difference(intersection(centroid(convexHull(bbox(buffer(wkt("POINT(0 0)"))
                                                                                        .maxDistance("10km")
                                                                                        .minDistance("1km")))))));
        String actual = geoShape("f", shape).operation("intersects").build();
        String expected = "{\"type\":\"geo_shape\",\"field\":\"f\",\"shape\":" +
                          "{\"type\":\"union\",\"shapes\":[" +
                          "{\"type\":\"difference\",\"shapes\":[" +
                          "{\"type\":\"intersection\",\"shapes\":[" +
                          "{\"type\":\"centroid\",\"shape\":" +
                          "{\"type\":\"convex_hull\",\"shape\":" +
                          "{\"type\":\"bbox\",\"shape\":" +
                          "{\"type\":\"buffer\",\"shape\":" +
                          "{\"type\":\"wkt\",\"value\":\"POINT(0 0)\"}," +
                          "\"max_distance\":\"10km\",\"min_distance\":\"1km\"}}}}]}]}]}," +
                          "\"operation\":\"intersects\"}";
        assertEquals("geo shape condition serialization is wrong", expected, actual);
    }

    @Test
    public void testSimpleSortFieldDefaults() {
        String actual = field("field1").build();
        String expected = "{\"type\":\"simple\",\"field\":\"field1\"}";
        assertEquals("sort field condition serialization is wrong", expected, actual);
    }

    @Test
    public void testSimpleSortFieldFull() {
        String actual = field("field1").reverse(true).build();
        String expected = "{\"type\":\"simple\",\"field\":\"field1\",\"reverse\":true}";
        assertEquals("sort field condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoDistanceSortFieldDefaults() {
        String actual = Builder.geoDistance("field1", 1.2, 3.4).build();
        String expected = "{\"type\":\"geo_distance\",\"field\":\"field1\",\"latitude\":1.2,\"longitude\":3.4}";
        assertEquals("sort field condition serialization is wrong", expected, actual);
    }

    @Test
    public void testGeoDistanceSortFieldFull() {
        String actual = Builder.geoDistance("field1", 1.2, -3.4).reverse(true).build();
        String expected = "{\"type\":\"geo_distance\"," +
                          "\"field\":\"field1\"," +
                          "\"latitude\":1.2," +
                          "\"longitude\":-3.4," +
                          "\"reverse\":true}";
        assertEquals("sort field condition serialization is wrong", expected, actual);
    }

    @Test
    public void testSortDefaults() {
        String actual = search().sort().build();
        String expected = "{\"sort\":[]}";
        assertEquals("sort condition serialization is wrong", expected, actual);
    }

    @Test
    public void testSortFull() {
        String actual = search().sort(field("field1"),
                                      field("field2"),
                                      Builder.geoDistance("field1", 1.0, -3.2).reverse(true)).build();
        String expected = "{\"sort\":[{" +
                          "\"type\":\"simple\",\"field\":\"field1\"}," +
                          "{\"type\":\"simple\",\"field\":\"field2\"}," +
                          "{\"type\":\"geo_distance\",\"field\":\"field1\"," +
                          "\"latitude\":1.0,\"longitude\":-3.2,\"reverse\":true}]}";
        assertEquals("sort condition serialization is wrong", expected, actual);
    }

    @Test
    public void testSearchDefaults() {
        String actual = search().build();
        String expected = "{}";
        assertEquals("search serialization is wrong", expected, actual);
    }

    @Test
    public void testSearchFull() {
        String actual = search().filter(match("f1", 1), match("f2", 2)).filter(match("f3", 3))
                                .query(match("f3", 3), match("f4", 4)).query(match("f5", 5))
                                .sort(field("f1"), field("f2")).sort(field("f3"))
                                .refresh(true)
                                .build();
        String expected = "{\"filter\":[" +
                          "{\"type\":\"match\",\"field\":\"f1\",\"value\":1}," +
                          "{\"type\":\"match\",\"field\":\"f2\",\"value\":2}," +
                          "{\"type\":\"match\",\"field\":\"f3\",\"value\":3}" +
                          "],\"query\":[" +
                          "{\"type\":\"match\",\"field\":\"f3\",\"value\":3}," +
                          "{\"type\":\"match\",\"field\":\"f4\",\"value\":4}," +
                          "{\"type\":\"match\",\"field\":\"f5\",\"value\":5}" +
                          "],\"sort\":[" +
                          "{\"type\":\"simple\",\"field\":\"f1\"}," +
                          "{\"type\":\"simple\",\"field\":\"f2\"}," +
                          "{\"type\":\"simple\",\"field\":\"f3\"}" +
                          "],\"refresh\":true}";
        assertEquals("search serialization is wrong", expected, actual);
    }

    @Test
    public void testSearchNestedBool() {
        String actual = search().filter(must(match("f1", 1)).should(match("f2", 2)).not(match("f3", 3)))
                                .query(must(match("f4", 4)).should(match("f5", 5)).not(match("f6", 6)))
                                .build();
        String expected = "{\"filter\":[" +
                          "{\"type\":\"boolean\"," +
                          "\"must\":[{\"type\":\"match\",\"field\":\"f1\",\"value\":1}]," +
                          "\"should\":[{\"type\":\"match\",\"field\":\"f2\",\"value\":2}]," +
                          "\"not\":[{\"type\":\"match\",\"field\":\"f3\",\"value\":3}]}]," +
                          "\"query\":[{\"type\":\"boolean\"," +
                          "\"must\":[{\"type\":\"match\",\"field\":\"f4\",\"value\":4}]," +
                          "\"should\":[{\"type\":\"match\",\"field\":\"f5\",\"value\":5}]," +
                          "\"not\":[{\"type\":\"match\",\"field\":\"f6\",\"value\":6}]}]}";
        assertEquals("search serialization is wrong", expected, actual);
    }

    @Test
    public void testToString() {
        String actual = range("field").build();
        String expected = range("field").build();
        assertEquals("to string is wrong", expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testFailingSerialization() {
        match("field", new ObjectMapper()).build();
    }

    @Test
    public void testIndexExample() {
        String actual = index("messages", "my_index").refreshSeconds(10)
                                                     .defaultAnalyzer("english")
                                                     .analyzer("danish", snowballAnalyzer("danish"))
                                                     .mapper("id", uuidMapper())
                                                     .mapper("user", stringMapper().caseSensitive(false))
                                                     .mapper("message", textMapper().analyzer("danish"))
                                                     .mapper("date", dateMapper().pattern("yyyyMMdd"))
                                                     .build();
        String expected = "CREATE CUSTOM INDEX my_index ON messages() USING 'com.stratio.cassandra.lucene.Index' " +
                          "WITH OPTIONS = {'refresh_seconds':'10','schema':'{\"default_analyzer\":\"english\"," +
                          "\"analyzers\":{\"danish\":{\"type\":\"snowball\",\"language\":\"danish\"}},\"fields\":" +
                          "{\"id\":{\"type\":\"uuid\"},\"user\":" +
                          "{\"type\":\"string\",\"case_sensitive\":false}," +
                          "\"message\":{\"type\":\"text\",\"analyzer\":\"danish\"}," +
                          "\"date\":{\"type\":\"date\",\"pattern\":\"yyyyMMdd\"}}}'}";
        assertEquals("index serialization is wrong", expected, actual);
    }

    @Test
    public void testSearchExample() {
        String actual = search().filter(match("user", "adelapena"))
                                .query(phrase("message", "cassandra rules"))
                                .sort(field("date").reverse(true))
                                .refresh(true)
                                .build();
        String expected = "{\"filter\":[{\"type\":\"match\",\"field\":\"user\",\"value\":\"adelapena\"}]," +
                          "\"query\":[{\"type\":\"phrase\",\"field\":\"message\",\"value\":\"cassandra rules\"}]," +
                          "\"sort\":[{\"type\":\"simple\",\"field\":\"date\",\"reverse\":true}],\"refresh\":true}";
        assertEquals("search serialization is wrong", expected, actual);
    }

}
