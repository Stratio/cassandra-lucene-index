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

package com.stratio.cassandra.lucene.schema.analysis;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SnowballAnalyzerBuilderTest {

    @Test(expected = IndexException.class)
    public void testBuildNullLanguage() {
        new SnowballAnalyzerBuilder(null, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildBlankLanguage() {
        new SnowballAnalyzerBuilder(" ", null);
    }

    @Test
    public void testBuildEnglish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("English", null);
        testAnalyzer(builder, "organization", "organ");
    }

    @Test
    public void testBuildFrench() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("French", null);
        testAnalyzer(builder, "contradictoirement", "contradictoir");
    }

    @Test
    public void testBuildSpanish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Spanish", null);
        testAnalyzer(builder, "perdido", "perd");
    }

    @Test
    public void testBuildPortuguese() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Portuguese", null);
        testAnalyzer(builder, "boataria", "boat");
    }

    @Test
    public void testBuildItalian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Italian", null);
        testAnalyzer(builder, "abbandoneranno", "abbandon");
    }

    @Test
    public void testBuildRomanian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Romanian", null);
        testAnalyzer(builder, "absolutul", "absol");
    }

    @Test
    public void testBuildGerman() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("German", null);
        testAnalyzer(builder, "katers", "kat");
    }

    @Test
    public void testBuildDanish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Danish", null);
        testAnalyzer(builder, "indtager", "indtag");
    }

    @Test
    public void testBuildDutch() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Dutch", null);
        testAnalyzer(builder, "opglimlachten", "opglimlacht");
    }

    @Test
    public void testBuilSwedish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Swedish", null);
        testAnalyzer(builder, "grejer", "grej");
    }

    @Test
    public void testBuildNorwegian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Norwegian", null);
        testAnalyzer(builder, "stuff", "stuff");
    }

    @Test
    public void testBuildRussian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Russian", null);
        testAnalyzer(builder, "kapta", "kapta");
    }

    @Test
    public void testBuildFinnish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Finnish", null);
        testAnalyzer(builder, "jutut", "jutu");
    }

    @Test
    public void testBuildIrish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Irish", null);
        testAnalyzer(builder, "stuif", "stuif");
    }

    @Test
    public void testBuildHungarian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Hungarian", null);
        testAnalyzer(builder, "dolog", "dolog");
    }

    @Test
    public void testBuildTurkish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Turkish", null);
        testAnalyzer(builder, "tekneler", "tekne");
    }

    @Test
    public void testBuildArmenian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Armenian", null);
        testAnalyzer(builder, "megy", "megy");
    }

    @Test
    public void testBuildBasque() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Basque", null);
        testAnalyzer(builder, "harrizko", "harri");
    }

    @Test
    public void testBuildCatalan() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Catalan", null);
        testAnalyzer(builder, "catalans", "catalan");
    }

    @Test(expected = RuntimeException.class)
    public void testBuildWithWrongLanguage() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("abc", null);
        testAnalyzer(builder, "organization", "organ");
    }

    @Test(expected = IndexException.class)
    public void testBuildWithoutLanguage() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder(null, null);
        testAnalyzer(builder, "organization", "organ");
    }

    @Test
    public void testParseJSONWithoutStopwords() throws IOException {
        String json = "{type:\"snowball\", language:\"English\"}";
        AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        testAnalyzer(builder, "the dogs are hungry", "dog", "hungri");
    }

    @Test
    public void testParseJSONWithStopwords() throws IOException {
        String json = "{type:\"snowball\", language:\"English\", stopwords:\"xx,yy\"}";
        AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        testAnalyzer(builder, "the dogs xx are hungry yy", "the", "dog", "are", "hungri");
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{class:\"abc\"}";
        JsonSerializer.fromString(json, AnalyzerBuilder.class);
    }

    private void testAnalyzer(AnalyzerBuilder builder, String value, String... expected) {
        Analyzer analyzer = builder.analyzer();
        assertNotNull("Expected not null analyzer", analyzer);
        List<String> tokens = analyze(value, analyzer);
        assertArrayEquals("Tokens are not the expected", expected, tokens.toArray());
        analyzer.close();
    }

    private List<String> analyze(String value, Analyzer analyzer) {
        List<String> result = new ArrayList<>();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(null, value);
            stream.reset();
            while (stream.incrementToken()) {
                String analyzedValue = stream.getAttribute(CharTermAttribute.class).toString();
                result.add(analyzedValue);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeWhileHandlingException(stream);
        }
        return result;
    }

}
