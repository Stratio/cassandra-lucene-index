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

package com.stratio.cassandra.lucene.builder;

import com.stratio.cassandra.lucene.builder.common.GeoTransformation;
import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.schema.Schema;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.ClasspathAnalyzer;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.SnowballAnalyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.*;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.*;
import com.stratio.cassandra.lucene.builder.search.sort.GeoDistanceSortField;
import com.stratio.cassandra.lucene.builder.search.sort.SimpleSortField;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;

/**
 * Abstract builder.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Builder {

    /** The embedded JSON serializer. */
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    static {
        jsonMapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonMapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
        jsonMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    /**
     * Returns the JSON {@code String} representation of the specified object.
     *
     * @return the JSON {@code String}
     */
    @Override
    public String toString() {
        return build();
    }

    /**
     * Returns the JSON representation of this {@link Builder}.
     *
     * @return a JSON representing this
     */
    public String build() {
        try {
            return jsonMapper.writeValueAsString(this);
        } catch (IOException e) {
            throw new BuilderException(e, "Error formatting JSON");
        }
    }

    /**
     * Returns a new index creation statement using the session's keyspace.
     *
     * @param table the table name
     * @param name the index name
     * @return a new index creation statement
     */
    public static Index index(String table, String name) {
        return new Index(table, name);
    }

    /**
     * Returns a new index creation statement using the session's keyspace.
     *
     * @param keyspace the keyspace name
     * @param table the table name
     * @param name the index name
     * @return a new index creation statement
     */
    public static Index index(String keyspace, String table, String name) {
        return new Index(table, name).keyspace(keyspace);
    }

    /**
     * Returns a new {@link Schema}.
     *
     * @return the {@link Schema}
     */
    public static Schema schema() {
        return new Schema();
    }

    /**
     * Returns a new {@link BigDecimalMapper}.
     *
     * @return a new {@link BigDecimalMapper}
     */
    public static BigDecimalMapper bigDecimalMapper() {
        return new BigDecimalMapper();
    }

    /**
     * Returns a new {@link BigIntegerMapper}.
     *
     * @return a new {@link BigIntegerMapper}
     */
    public static BigIntegerMapper bigIntegerMapper() {
        return new BigIntegerMapper();
    }

    /**
     * Returns a new {@link BitemporalMapper}.
     *
     * @param vtFrom the column name containing the valid time start
     * @param vtTo the column name containing the valid time stop
     * @param ttFrom the column name containing the transaction time start
     * @param ttTo the column name containing the transaction time stop
     * @return a new {@link BitemporalMapper}
     */
    public static BitemporalMapper bitemporalMapper(String vtFrom, String vtTo, String ttFrom, String ttTo) {
        return new BitemporalMapper(vtFrom, vtTo, ttFrom, ttTo);
    }

    /**
     * Returns a new {@link BlobMapper}.
     *
     * @return a new blob mapper
     */
    public static BlobMapper blobMapper() {
        return new BlobMapper();
    }

    /**
     * Returns a new {@link BooleanMapper}.
     *
     * @return a new boolean mapper.
     */
    public static BooleanMapper booleanMapper() {
        return new BooleanMapper();
    }

    /**
     * Returns a new {@link DateMapper}.
     *
     * @return a new date mapper
     */
    public static DateMapper dateMapper() {
        return new DateMapper();
    }

    /**
     * Returns a new {@link DateRangeMapper}.
     *
     * @param from the column containing the start date
     * @param to the column containing the end date
     * @return a new {@link DateRangeMapper}
     */
    public static DateRangeMapper dateRangeMapper(String from, String to) {
        return new DateRangeMapper(from, to);
    }

    /**
     * Returns a new {@link DoubleMapper}.
     *
     * @return a new double mapper
     */
    public static DoubleMapper doubleMapper() {
        return new DoubleMapper();
    }

    /**
     * Returns a new {@link FloatMapper}.
     *
     * @return a new float mapper
     */
    public static FloatMapper floatMapper() {
        return new FloatMapper();
    }

    /**
     * Returns a new {@link GeoPointMapper}.
     *
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @return a new geo point mapper
     */
    public static GeoPointMapper geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapper(latitude, longitude);
    }

    /**
     * Returns a new {@link GeoShapeMapper}.
     *
     * @return a new geo shape mapper
     */
    public static GeoShapeMapper geoShapeMapper() {
        return new GeoShapeMapper();
    }

    /**
     * Returns a new {@link InetMapper}.
     *
     * @return a new inet mapper
     */
    public static InetMapper inetMapper() {
        return new InetMapper();
    }

    /**
     * Returns a new {@link IntegerMapper}.
     *
     * @return a new integer mapper
     */
    public static IntegerMapper integerMapper() {
        return new IntegerMapper();
    }

    /**
     * Returns a new {@link LongMapper}.
     *
     * @return a new long mapper
     */
    public static LongMapper longMapper() {
        return new LongMapper();
    }

    /**
     * Returns a new {@link StringMapper}.
     *
     * @return a new string mapper
     */
    public static StringMapper stringMapper() {
        return new StringMapper();
    }

    /**
     * Returns a new {@link TextMapper}.
     *
     * @return a new text mapper
     */
    public static TextMapper textMapper() {
        return new TextMapper();
    }

    /**
     * Returns a new {@link UUIDMapper}.
     *
     * @return a new UUID mapper
     */
    public static UUIDMapper uuidMapper() {
        return new UUIDMapper();
    }

    /**
     * Returns a new {@link ClasspathAnalyzer}.
     *
     * @param className the Lucene's {@code Analyzer} full class name.
     * @return a new classpath analyzer
     */
    public static ClasspathAnalyzer classpathAnalyzer(String className) {
        return new ClasspathAnalyzer(className);
    }

    /**
     * Returns a new {@link SnowballAnalyzer} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @return a new snowball analyzer
     */
    public static SnowballAnalyzer snowballAnalyzer(String language) {
        return new SnowballAnalyzer(language);
    }

    /**
     * Returns a new {@link Search}.
     *
     * @return a new search
     */
    public static Search search() {
        return new Search();
    }

    /**
     * Returns a new {@link AllCondition}.
     *
     * @return a new all condition
     */
    public static AllCondition all() {
        return new AllCondition();
    }

    /**
     * Returns a new {@link BitemporalCondition} for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new bitemporal condition
     */
    public static BitemporalCondition bitemporal(String field) {
        return new BitemporalCondition(field);
    }

    /**
     * Returns a new {@link BooleanCondition}.
     *
     * @return a new boolean condition
     */
    public static BooleanCondition bool() {
        return new BooleanCondition();
    }

    /**
     * Returns a new {@link ContainsCondition}.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     * @return a new conatins condition
     */
    public static ContainsCondition contains(String field, Object... values) {
        return new ContainsCondition(field, values);
    }

    /**
     * Returns a new {@link FuzzyCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new fuzzy condition
     */
    public static FuzzyCondition fuzzy(String field, String value) {
        return new FuzzyCondition(field, value);
    }

    /**
     * Returns a new {@link LuceneCondition} with the specified query.
     *
     * @param query the Lucene syntax query
     * @return a new Lucene condition
     */
    public static LuceneCondition lucene(String query) {
        return new LuceneCondition(query);
    }

    /**
     * Returns a new {@link MatchCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new match condition
     */
    public static MatchCondition match(String field, Object value) {
        return new MatchCondition(field, value);
    }

    /**
     * Returns a new {@link NoneCondition}.
     *
     * @return a new none condition
     */
    public static NoneCondition none() {
        return new NoneCondition();
    }

    /**
     * Returns a new {@link PhraseCondition} for the specified field and values.
     *
     * @param field the name of the field to be matched
     * @param value the text to be matched
     * @return a new phrase condition
     */
    public static PhraseCondition phrase(String field, String value) {
        return new PhraseCondition(field, value);
    }

    /**
     * Returns a new {@link PrefixCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new prefix condition
     */
    public static PrefixCondition prefix(String field, String value) {
        return new PrefixCondition(field, value);
    }

    /**
     * Returns a new {@link RangeCondition} for the specified field.
     *
     * @param field the name of the field to be matched
     * @return a new range condition
     */
    public static RangeCondition range(String field) {
        return new RangeCondition(field);
    }

    /**
     * Returns a new {@link RegexpCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new regexp condition
     */
    public static RegexpCondition regexp(String field, String value) {
        return new RegexpCondition(field, value);
    }

    /**
     * Returns a new {@link WildcardCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new wildcard condition
     */
    public static WildcardCondition wildcard(String field, String value) {
        return new WildcardCondition(field, value);
    }

    /**
     * Returns a new {@link GeoBBoxCondition} with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLongitude the minimum accepted longitude
     * @param maxLongitude the maximum accepted longitude
     * @param minLatitude the minimum accepted latitude
     * @param maxLatitude the maximum accepted latitude
     * @return a new geo bounding box condition
     */
    public static GeoBBoxCondition geoBBox(String field,
                                           double minLongitude,
                                           double maxLongitude,
                                           double minLatitude,
                                           double maxLatitude) {
        return new GeoBBoxCondition(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }

    /**
     * Returns a new {@link GeoDistanceCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @param longitude the longitude of the reference point
     * @param latitude the latitude of the reference point
     * @param maxDistance the max allowed distance
     * @return a new geo distance condition
     */
    public static GeoDistanceCondition geoDistance(String field,
                                                   double longitude,
                                                   double latitude,
                                                   String maxDistance) {
        return new GeoDistanceCondition(field, latitude, longitude, maxDistance);
    }

    /**
     * Returns a new {@link GeoShapeCondition} with the specified field reference point.
     *
     * @param field the name of the field
     * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
     * @return a new geos hape condition
     */
    public static GeoShapeCondition geoShape(String field, String shape) {
        return new GeoShapeCondition(field, shape);
    }

    /**
     * Returns a new {@link GeoTransformation.Buffer}.
     *
     * @return a new buffer transformation
     */
    public static GeoTransformation.Buffer bufferGeoTransformation() {
        return new GeoTransformation.Buffer();
    }

    /**
     * Returns a new {@link GeoTransformation.Centroid}.
     *
     * @return a new buffer transformation
     */
    public static GeoTransformation.Centroid centroidGeoTransformation() {
        return new GeoTransformation.Centroid();
    }

    /**
     * Returns a new {@link GeoTransformation.Difference}.
     *
     * @param shape the shape to be subtracted
     * @return a new difference transformation
     */
    public static GeoTransformation.Difference differenceGeoTransformation(String shape) {
        return new GeoTransformation.Difference(shape);
    }

    /**
     * Returns a new {@link GeoTransformation.Intersection}.
     *
     * @param shape the shape to be intersected
     * @return a new intersection transformation
     */
    public static GeoTransformation.Intersection intersectionGeoTransformation(String shape) {
        return new GeoTransformation.Intersection(shape);
    }

    /**
     * Returns a new {@link GeoTransformation.Union}.
     *
     * @param shape the shape to be added
     * @return a new union transformation
     */
    public static GeoTransformation.Union unionGeoTransformation(String shape) {
        return new GeoTransformation.Union(shape);
    }

    /**
     * Returns a new {@link DateRangeCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @return a new date range condition
     */
    public static DateRangeCondition dateRange(String field) {
        return new DateRangeCondition(field);
    }

    /**
     * Returns a new {@link SimpleSortField} for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new simple sort field
     */
    public static SimpleSortField field(String field) {
        return new SimpleSortField(field);
    }

    /**
     * Returns a new {@link GeoDistanceSortField} for the specified field.
     *
     * @param mapper the name of the field to be used for sort
     * @param longitude the longitude in degrees of the point to min distance sort by
     * @param latitude the latitude in degrees of the point to min distance sort by
     * @return a new geo distance sort field
     */
    public static GeoDistanceSortField geoDistanceField(String mapper, double longitude, double latitude) {
        return new GeoDistanceSortField(mapper, longitude, latitude);
    }
}
