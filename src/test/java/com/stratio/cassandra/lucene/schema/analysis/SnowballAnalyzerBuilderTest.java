/*
 * Copyright 2015, Stratio.
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
package com.stratio.cassandra.lucene.schema.analysis;

import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SnowballAnalyzerBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullLanguage() {
        new SnowballAnalyzerBuilder(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildBlankLanguage() {
        new SnowballAnalyzerBuilder(" ", null);
    }

    @Test
    public void testBuildEnglish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("English", null);
        testAnalyzer("organization", "organ", builder);
    }

    @Test
    public void testBuildFrench() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("French", null);
        testAnalyzer("contradictoirement", "contradictoir", builder);
    }

    @Test
    public void testBuildSpanish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Spanish", null);
        testAnalyzer("perdido", "perd", builder);
    }

    @Test
    public void testBuildPortuguese() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Portuguese", null);
        testAnalyzer("boataria", "boat", builder);
    }

    @Test
    public void testBuildItalian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Italian", null);
        testAnalyzer("abbandoneranno", "abbandon", builder);
    }

    @Test
    public void testBuildRomanian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Romanian", null);
        testAnalyzer("absolutul", "absol", builder);
    }

    @Test
    public void testBuildGerman() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("German", null);
        testAnalyzer("katers", "kat", builder);
    }

    @Test
    public void testBuildDanish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Danish", null);
        testAnalyzer("indtager", "indtag", builder);
    }

    @Test
    public void testBuildDutch() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Dutch", null);
        testAnalyzer("opglimlachten", "opglimlacht", builder);
    }

    @Test
    public void testBuilSwedish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Swedish", null);
        testAnalyzer("grejer", "grej", builder);
    }

    @Test
    public void testBuildNorwegian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Norwegian", null);
        testAnalyzer("stuff", "stuff", builder);
    }

    @Test
    public void testBuildRussian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Russian", null);
        testAnalyzer("kapta", "kapta", builder);
    }

    @Test
    public void testBuildFinnish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Finnish", null);
        testAnalyzer("jutut", "jutu", builder);
    }

    @Test
    public void testBuildIrish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Irish", null);
        testAnalyzer("stuif", "stuif", builder);
    }

    @Test
    public void testBuildHungarian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Hungarian", null);
        testAnalyzer("dolog", "dolog", builder);
    }

    @Test
    public void testBuildTurkish() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Turkish", null);
        testAnalyzer("tekneler", "tekne", builder);
    }

    @Test
    public void testBuildArmenian() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Armenian", null);
        testAnalyzer("megy", "megy", builder);
    }

    @Test
    public void testBuildBasque() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Basque", null);
        testAnalyzer("harrizko", "harri", builder);
    }

    @Test
    public void testBuildCatalan() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("Catalan", null);
        testAnalyzer("catalans", "catalan", builder);
    }

    @Test(expected = RuntimeException.class)
    public void testBuildWithWrongLanguage() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder("abc", null);
        testAnalyzer("organization", "organ", builder);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutLanguage() {
        AnalyzerBuilder builder = new SnowballAnalyzerBuilder(null, null);
        testAnalyzer("organization", "organ", builder);
    }

    @Test
    public void testParseJSONWithoutStopwords() throws IOException {
        String json = "{type:\"snowball\", language:\"English\"}";
        AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        testAnalyzer("the dogs are hungry", "dog hungri", builder);
    }

    @Test
    public void testParseJSONWithStopwords() throws IOException {
        String json = "{type:\"snowball\", language:\"English\", stopwords:\"xx,yy\"}";
        AnalyzerBuilder builder = JsonSerializer.fromString(json, AnalyzerBuilder.class);
        testAnalyzer("the dogs xx are hungry yy", "the dog are hungri", builder);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{class:\"abc\"}";
        JsonSerializer.fromString(json, AnalyzerBuilder.class);
    }

    private void testAnalyzer(String value, String expected, AnalyzerBuilder builder) {
        Analyzer analyzer = builder.analyzer();
        Assert.assertNotNull(analyzer);
        String actual = AnalysisUtils.instance.analyzeAsText(value, analyzer);
        Assert.assertEquals(expected, actual);
        analyzer.close();
    }
}
