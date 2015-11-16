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

import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.schema.Schema;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.ClasspathAnalyzer;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.SnowballAnalyzer;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.*;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.*;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
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
        jsonMapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonMapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
        jsonMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    /**
     * Returns the JSON {@code String} representation of the specified object.
     *
     * @return The JSON {@code String} representation of the specified object.
     */
    @Override
    public String toString() {
        return build();
    }

    /**
     * Returns the JSON representation of this {@link Builder}.
     *
     * @return The JSON representation of this {@link Builder}.
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
     * @param table  The table name.
     * @param column The indexed column name.
     * @return A new index creation statement.
     */
    public static Index index(String table, String column) {
        return new Index(table, column);
    }

    /**
     * Returns a new index creation statement using the session's keyspace.
     *
     * @param keyspace The keyspace name.
     * @param table    The table name.
     * @param column   The indexed column name.
     * @return A new index creation statement.
     */
    public static Index index(String keyspace, String table, String column) {
        return new Index(table, column).keyspace(keyspace);
    }

    /**
     * Returns a new {@link Schema}.
     *
     * @return A new {@link Schema}.
     */
    public static Schema schema() {
        return new Schema();
    }

    /**
     * Returns a new {@link BigDecimalMapper}.
     *
     * @return A new {@link BigDecimalMapper}.
     */
    public static BigDecimalMapper bigDecimalMapper() {
        return new BigDecimalMapper();
    }

    /**
     * Returns a new {@link BigIntegerMapper}.
     *
     * @return A new {@link BigIntegerMapper}.
     */
    public static BigIntegerMapper bigIntegerMapper() {
        return new BigIntegerMapper();
    }

    /**
     * Returns a new {@link BitemporalMapper}.
     *
     * @param vtFrom The column name containing the valid time start.
     * @param vtTo   The column name containing the valid time stop.
     * @param ttFrom The column name containing the transaction time start.
     * @param ttTo   The column name containing the transaction time stop.
     * @return A new {@link BitemporalMapper}.
     */
    public static BitemporalMapper bitemporalMapper(String vtFrom, String vtTo, String ttFrom, String ttTo) {
        return new BitemporalMapper(vtFrom, vtTo, ttFrom, ttTo);
    }

    /**
     * Returns a new {@link BlobMapper}.
     *
     * @return A new {@link BlobMapper}.
     */
    public static BlobMapper blobMapper() {
        return new BlobMapper();
    }

    /**
     * Returns a new {@link BooleanMapper}.
     *
     * @return A new {@link BooleanMapper}.
     */
    public static BooleanMapper booleanMapper() {
        return new BooleanMapper();
    }

    /**
     * Returns a new {@link DateMapper}.
     *
     * @return A new {@link DateMapper}.
     */
    public static DateMapper dateMapper() {
        return new DateMapper();
    }

    /**
     * Returns a new {@link DateRangeMapper}.
     *
     * @param from The column containing the start date.
     * @param to   The column containing the end date.
     * @return A new {@link DateRangeMapper}.
     */
    public static DateRangeMapper dateRangeMapper(String from, String to) {
        return new DateRangeMapper(from, to);
    }

    /**
     * Returns a new {@link DoubleMapper}.
     *
     * @return A new {@link DoubleMapper}.
     */
    public static DoubleMapper doubleMapper() {
        return new DoubleMapper();
    }

    /**
     * Returns a new {@link FloatMapper}.
     *
     * @return A new {@link FloatMapper}.
     */
    public static FloatMapper floatMapper() {
        return new FloatMapper();
    }

    /**
     * Returns a new {@link GeoPointMapper}.
     *
     * @param latitude  The name of the column containing the latitude.
     * @param longitude The name of the column containing the longitude.
     * @return A new {@link GeoPointMapper}.
     */
    public static GeoPointMapper geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapper(latitude, longitude);
    }

    /**
     * Returns a new {@link InetMapper}.
     *
     * @return A new {@link InetMapper}.
     */
    public static InetMapper inetMapper() {
        return new InetMapper();
    }

    /**
     * Returns a new {@link IntegerMapper}.
     *
     * @return A new {@link IntegerMapper}.
     */
    public static IntegerMapper integerMapper() {
        return new IntegerMapper();
    }

    /**
     * Returns a new {@link LongMapper}.
     *
     * @return A new {@link LongMapper}.
     */
    public static LongMapper longMapper() {
        return new LongMapper();
    }

    /**
     * Returns a new {@link StringMapper}.
     *
     * @return A new {@link StringMapper}.
     */
    public static StringMapper stringMapper() {
        return new StringMapper();
    }

    /**
     * Returns a new {@link TextMapper}.
     *
     * @return A new {@link TextMapper}.
     */
    public static TextMapper textMapper() {
        return new TextMapper();
    }

    /**
     * Returns a new {@link UUIDMapper}.
     *
     * @return A new {@link UUIDMapper}.
     */
    public static UUIDMapper uuidMapper() {
        return new UUIDMapper();
    }

    /**
     * Returns a new {@link ClasspathAnalyzer}.
     *
     * @param className An Lucene's {@code Analyzer} full class name.
     * @return A new {@link ClasspathAnalyzer}.
     */
    public static ClasspathAnalyzer classpathAnalyzer(String className) {
        return new ClasspathAnalyzer(className);
    }

    /**
     * Returns a new {@link SnowballAnalyzer} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     *                 Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     *                 Turkish, Armenian, Basque and Catalan.
     * @return A new {@link SnowballAnalyzer}.
     */
    public static SnowballAnalyzer snowballAnalyzer(String language) {
        return new SnowballAnalyzer(language);
    }

    /**
     * Returns a new {@link Search}.
     *
     * @return a new {@link Search}.
     */
    public static Search search() {
        return new Search();
    }

    /**
     * Returns a new {@link AllCondition} for the specified field and value.
     *
     * @return A new {@link AllCondition} for the specified field and value.
     */
    public static AllCondition all() {
        return new AllCondition();
    }

    /**
     * Returns a new {@link BitemporalCondition} for the specified field.
     *
     * @param field The name of the field to be sorted.
     * @return A new {@link BitemporalCondition} for the specified field.
     */
    public static BitemporalCondition bitemporal(String field) {
        return new BitemporalCondition(field);
    }

    /**
     * Returns a new {@link BooleanCondition}.
     *
     * @return A new {@link BooleanCondition}.
     */
    public static BooleanCondition bool() {
        return new BooleanCondition();
    }

    /**
     * Returns a new {@link ContainsCondition}.
     *
     * @param field  The name of the field to be matched.
     * @param values The values of the field to be matched.
     * @return A new {@link ContainsCondition}.
     */
    public static ContainsCondition contains(String field, Object... values) {
        return new ContainsCondition(field, values);
    }

    /**
     * Returns a new {@link FuzzyCondition} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link FuzzyCondition} for the specified field and value.
     */
    public static FuzzyCondition fuzzy(String field, String value) {
        return new FuzzyCondition(field, value);
    }

    /**
     * Returns a new {@link LuceneCondition} with the specified query.
     *
     * @param query The Lucene syntax query.
     * @return A new {@link LuceneCondition} with the specified query.
     */
    public static LuceneCondition lucene(String query) {
        return new LuceneCondition(query);
    }

    /**
     * Returns a new {@link MatchCondition} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link MatchCondition} for the specified field and value.
     */
    public static MatchCondition match(String field, Object value) {
        return new MatchCondition(field, value);
    }

    /**
     * Returns a new {@link NoneCondition} for the specified field and value.
     *
     * @return A new {@link NoneCondition} for the specified field and value.
     */
    public static NoneCondition none() {
        return new NoneCondition();
    }

    /**
     * Returns a new {@link PhraseCondition} for the specified field and values.
     *
     * @param field The name of the field to be matched.
     * @param value The text to be matched.
     * @return A new {@link PhraseCondition} for the specified field and values.
     */
    public static PhraseCondition phrase(String field, String value) {
        return new PhraseCondition(field, value);
    }

    /**
     * Returns a new {@link PrefixCondition} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link PrefixCondition} for the specified field and value.
     */
    public static PrefixCondition prefix(String field, String value) {
        return new PrefixCondition(field, value);
    }

    /**
     * Returns a new {@link RangeCondition} for the specified field.
     *
     * @param field The name of the field to be matched.
     * @return A new {@link RangeCondition} for the specified field.
     */
    public static RangeCondition range(String field) {
        return new RangeCondition(field);
    }

    /**
     * Returns a new {@link RegexpCondition} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link RegexpCondition} for the specified field and value.
     */
    public static RegexpCondition regexp(String field, String value) {
        return new RegexpCondition(field, value);
    }

    /**
     * Returns a new {@link WildcardCondition} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link WildcardCondition} for the specified field and value.
     */
    public static WildcardCondition wildcard(String field, String value) {
        return new WildcardCondition(field, value);
    }

    /**
     * Returns a new {@link GeoBBoxCondition} with the specified field name and bounding box coordinates.
     *
     * @param field        The name of the field to be matched.
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     * @param minLatitude  The minimum accepted latitude.
     * @param maxLatitude  The maximum accepted latitude.
     * @return A new {@link GeoBBoxCondition}.
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
     * @param field       The name of the field to be matched.
     * @param longitude   The longitude of the reference point.
     * @param latitude    The latitude of the reference point.
     * @param maxDistance The max allowed distance.
     * @return A new {@link GeoDistanceCondition}.
     */
    public static GeoDistanceCondition geoDistance(String field,
                                                   double longitude,
                                                   double latitude,
                                                   String maxDistance) {
        return new GeoDistanceCondition(field, latitude, longitude, maxDistance);
    }

    /**
     * Returns a new {@link DateRangeCondition} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     * @return A new {@link DateRangeCondition}.
     */
    public static DateRangeCondition dateRange(String field) {
        return new DateRangeCondition(field);
    }

    /**
     * Returns a new {@link SortField} for the specified field.
     *
     * @param field The name of the field to be sorted.
     * @return A new {@link SortField} for the specified field.
     */
    public static SortField field(String field) {
        return new SortField(field);
    }
}
