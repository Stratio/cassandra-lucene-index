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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class AnalysisUtilsTest {

    private static Analyzer englishAnalyzer;
    private static Analyzer spanishAnalyzer;
    private static Analyzer perFieldAnalyzer;

    @BeforeClass
    public static void beforeClass() {
        englishAnalyzer = new EnglishAnalyzer();
        spanishAnalyzer = new SpanishAnalyzer();
        Map<String, Analyzer> analyzers = new HashMap<>();
        analyzers.put("english", englishAnalyzer);
        analyzers.put("spanish", spanishAnalyzer);
        perFieldAnalyzer = new PerFieldAnalyzerWrapper(spanishAnalyzer, analyzers);
    }

    @AfterClass
    public static void afterClass() {
        perFieldAnalyzer.close();
        englishAnalyzer.close();
        spanishAnalyzer.close();
    }

    @Test
    public void testAnalyzeAsTokensWithNullField() {
        String text = "the lazy dog jumps over the quick brown fox";
        List<String> analyzedText = AnalysisUtils.analyzeAsTokens(null, text, englishAnalyzer);
        List<String> expectedText = Arrays.asList("lazi", "dog", "jump", "over", "quick", "brown", "fox");
        Assert.assertEquals(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTokensWithField() {
        String text = "the lazy dog jumps over the quick brown fox";
        List<String> analyzedText = AnalysisUtils.analyzeAsTokens("english", text, perFieldAnalyzer);
        List<String> expectedText = Arrays.asList("lazi", "dog", "jump", "over", "quick", "brown", "fox");
        Assert.assertEquals(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTokensWithWrongField() {
        String text = "the lazy dog jumps over the quick brown fox";
        List<String> analyzedText = AnalysisUtils.analyzeAsTokens(null, text, perFieldAnalyzer);
        List<String> expectedText = Arrays.asList("lazi", "dog", "jump", "over", "quick", "brown", "fox");
        Assert.assertNotSame(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTokensWithoutField() {
        String text = "the lazy dog jumps over the quick brown fox";
        List<String> analyzedText = AnalysisUtils.analyzeAsTokens(text, englishAnalyzer);
        List<String> expectedText = Arrays.asList("lazi", "dog", "jump", "over", "quick", "brown", "fox");
        Assert.assertEquals(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTextWithNullField() {
        String text = "the lazy dog jumps over the quick brown fox";
        String analyzedText = AnalysisUtils.analyzeAsText(null, text, englishAnalyzer);
        String expectedText = "lazi dog jump over quick brown fox";
        Assert.assertEquals(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTextWithField() {
        String text = "the lazy dog jumps over the quick brown fox";
        String analyzedText = AnalysisUtils.analyzeAsText("english", text, perFieldAnalyzer);
        String expectedText = "lazi dog jump over quick brown fox";
        Assert.assertEquals(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTextWithWrongField() {
        String text = "the lazy dog jumps over the quick brown fox";
        String analyzedText = AnalysisUtils.analyzeAsText(null, text, perFieldAnalyzer);
        String expectedText = "lazi dog jump over quick brown fox";
        Assert.assertNotSame(expectedText, analyzedText);
    }

    @Test
    public void testAnalyzeAsTextWithoutField() {
        String text = "the lazy dog jumps over the quick brown fox";
        String analyzedText = AnalysisUtils.analyzeAsText(text, englishAnalyzer);
        String expectedText = "lazi dog jump over quick brown fox";
        Assert.assertEquals(expectedText, analyzedText);
    }

}
