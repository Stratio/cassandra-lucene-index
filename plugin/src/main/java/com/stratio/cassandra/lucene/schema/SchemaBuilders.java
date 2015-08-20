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

package com.stratio.cassandra.lucene.schema;

import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.analysis.SnowballAnalyzerBuilder;
import com.stratio.cassandra.lucene.schema.mapping.builder.*;

import java.util.LinkedHashMap;

/**
 * Class centralizing several {@link Schema} related builders.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class SchemaBuilders {

    /** Private constructor to hide the implicit public one. */
    private SchemaBuilders() {
    }

    /**
     * Returns a new {@link SchemaBuilder}.
     *
     * @return A new {@link SchemaBuilder}.
     */
    public static SchemaBuilder schema() {
        return new SchemaBuilder(null,
                                 new LinkedHashMap<String, AnalyzerBuilder>(),
                                 new LinkedHashMap<String, MapperBuilder<?>>());
    }

    /**
     * Returns a new {@link BigDecimalMapperBuilder}.
     *
     * @return A new {@link BigDecimalMapperBuilder}.
     */
    public static BigDecimalMapperBuilder bigDecimalMapper() {
        return new BigDecimalMapperBuilder();
    }

    /**
     * Returns a new {@link BigIntegerMapperBuilder}.
     *
     * @return A new {@link BigIntegerMapperBuilder}.
     */
    public static BigIntegerMapperBuilder bigIntegerMapper() {
        return new BigIntegerMapperBuilder();
    }

    /**
     * Returns a new {@link BitemporalMapperBuilder}.
     *
     * @param vtFrom The column name containing the valid time start.
     * @param vtTo   The column name containing the valid time stop.
     * @param ttFrom The column name containing the transaction time start.
     * @param ttTo   The column name containing the transaction time stop.
     * @return A new {@link BitemporalMapperBuilder}.
     */
    public static BitemporalMapperBuilder bitemporalMapper(String vtFrom, String vtTo, String ttFrom, String ttTo) {
        return new BitemporalMapperBuilder(vtFrom, vtTo, ttFrom, ttTo);
    }

    /**
     * Returns a new {@link BlobMapperBuilder}.
     *
     * @return A new {@link BlobMapperBuilder}.
     */
    public static BlobMapperBuilder blobMapper() {
        return new BlobMapperBuilder();
    }

    /**
     * Returns a new {@link BooleanMapperBuilder}.
     *
     * @return A new {@link BooleanMapperBuilder}.
     */
    public static BooleanMapperBuilder booleanMapper() {
        return new BooleanMapperBuilder();
    }

    /**
     * Returns a new {@link DateMapperBuilder}.
     *
     * @return A new {@link DateMapperBuilder}.
     */
    public static DateMapperBuilder dateMapper() {
        return new DateMapperBuilder();
    }

    /**
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param from The column containing the start date.
     * @param to   The column containing the end date.
     * @return A new {@link DateRangeMapperBuilder}.
     */
    public static DateRangeMapperBuilder dateRangeMapper(String from, String to) {
        return new DateRangeMapperBuilder(from, to);
    }

    /**
     * Returns a new {@link DoubleMapperBuilder}.
     *
     * @return A new {@link DoubleMapperBuilder}.
     */
    public static DoubleMapperBuilder doubleMapper() {
        return new DoubleMapperBuilder();
    }

    /**
     * Returns a new {@link FloatMapperBuilder}.
     *
     * @return A new {@link FloatMapperBuilder}.
     */
    public static FloatMapperBuilder floatMapper() {
        return new FloatMapperBuilder();
    }

    /**
     * Returns a new {@link GeoPointMapperBuilder}.
     *
     * @param latitude  The name of the column containing the latitude.
     * @param longitude The name of the column containing the longitude.
     * @return A new {@link GeoPointMapperBuilder}.
     */
    public static GeoPointMapperBuilder geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapperBuilder(latitude, longitude);
    }

    /**
     * Returns a new {@link InetMapperBuilder}.
     *
     * @return A new {@link InetMapperBuilder}.
     */
    public static InetMapperBuilder inetMapper() {
        return new InetMapperBuilder();
    }

    /**
     * Returns a new {@link IntegerMapperBuilder}.
     *
     * @return A new {@link IntegerMapperBuilder}.
     */
    public static IntegerMapperBuilder integerMapper() {
        return new IntegerMapperBuilder();
    }

    /**
     * Returns a new {@link LongMapperBuilder}.
     *
     * @return A new {@link LongMapperBuilder}.
     */
    public static LongMapperBuilder longMapper() {
        return new LongMapperBuilder();
    }

    /**
     * Returns a new {@link StringMapperBuilder}.
     *
     * @return A new {@link StringMapperBuilder}.
     */
    public static StringMapperBuilder stringMapper() {
        return new StringMapperBuilder();
    }

    /**
     * Returns a new {@link TextMapperBuilder}.
     *
     * @return A new {@link TextMapperBuilder}.
     */
    public static TextMapperBuilder textMapper() {
        return new TextMapperBuilder();
    }

    /**
     * Returns a new {@link UUIDMapperBuilder}.
     *
     * @return A new {@link UUIDMapperBuilder}.
     */
    public static UUIDMapperBuilder uuidMapper() {
        return new UUIDMapperBuilder();
    }

    /**
     * Returns a new {@link ClasspathAnalyzerBuilder}.
     *
     * @param className An {@link org.apache.lucene.analysis.Analyzer} full class name.
     * @return A new {@link ClasspathAnalyzerBuilder}.
     */
    public static ClasspathAnalyzerBuilder classpathAnalyzer(String className) {
        return new ClasspathAnalyzerBuilder(className);
    }

    /**
     * Returns a new {@link SnowballAnalyzerBuilder} for the specified language and stopwords.
     *
     * @param language  The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     *                  Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     *                  Turkish, Armenian, Basque and Catalan.
     * @param stopwords The comma separated stopwords {@code String}.
     * @return A new {@link SnowballAnalyzerBuilder}.
     */
    public static SnowballAnalyzerBuilder snowballAnalyzer(String language, String stopwords) {
        return new SnowballAnalyzerBuilder(language, stopwords);
    }

}
