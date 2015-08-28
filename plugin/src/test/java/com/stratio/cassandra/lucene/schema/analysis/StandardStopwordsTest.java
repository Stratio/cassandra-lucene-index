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

import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class StandardStopwordsTest {

    @Test
    public void testGetArmenianPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.ARMENIAN.get();
        assertEquals("Expected another stopwords", ArmenianAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetBasquePreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.BASQUE.get();
        assertEquals("Expected another stopwords", BasqueAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetCaatalanPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.CATALAN.get();
        assertEquals("Expected another stopwords", CatalanAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetDutchPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.DUTCH.get();
        assertEquals("Expected another stopwords", DutchAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetDanishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.DANISH.get();
        assertEquals("Expected another stopwords", DanishAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetEnglishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.ENGLISH.get();
        assertEquals("Expected another stopwords", EnglishAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetFinnishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.FINNISH.get();
        assertEquals("Expected another stopwords", FinnishAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetFrenchPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.FRENCH.get();
        assertEquals("Expected another stopwords", FrenchAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetGermanPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.GERMAN.get();
        assertEquals("Expected another stopwords", GermanAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetHungarianPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.HUNGARIAN.get();
        assertEquals("Expected another stopwords", HungarianAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetIrishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.IRISH.get();
        assertEquals("Expected another stopwords", IrishAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetItalianPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.ITALIAN.get();
        assertEquals("Expected another stopwords", ItalianAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetNorwegianPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.NORWEGIAN.get();
        assertEquals("Expected another stopwords", NorwegianAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetPortuguesePreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.PORTUGUESE.get();
        assertEquals("Expected another stopwords", PortugueseAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetRussianPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.RUSSIAN.get();
        assertEquals("Expected another stopwords", RussianAnalyzer.getDefaultStopSet(), stopwords);
    }

    @Test
    public void testGetSpanishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.SPANISH.get();
        assertEquals("Expected another stopwords", SpanishAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetSwedishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.SWEDISH.get();
        assertEquals("Expected another stopwords", SwedishAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetTurkishPreBuiltAnalyzer() {
        CharArraySet stopwords = StandardStopwords.TURKISH.get();
        assertEquals("Expected another stopwords", TurkishAnalyzer.getDefaultStopSet(), stopwords);

    }

    @Test
    public void testGetStandardStopwordsFromNameLowerCase() {
        CharArraySet stopwords = StandardStopwords.get("english");
        assertEquals("Expected not null stopwords", stopwords, EnglishAnalyzer.getDefaultStopSet());

    }

    @Test
    public void testGetStandardStopwordsFromNameUpperCase() {
        CharArraySet stopwords = StandardStopwords.get("English");
        assertEquals("Expected not null stopwords", stopwords, EnglishAnalyzer.getDefaultStopSet());
    }

    @Test
    public void testGetStandardStopwordsUnexistent() {
        CharArraySet stopwords = StandardStopwords.get("unexistent");
        assertEquals("Expected null stopwords", stopwords, CharArraySet.EMPTY_SET);
    }

}
