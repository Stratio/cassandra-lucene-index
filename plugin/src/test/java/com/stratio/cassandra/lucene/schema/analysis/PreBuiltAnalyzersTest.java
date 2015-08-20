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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PreBuiltAnalyzersTest {

    @Test
    public void testGetStandardPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.STANDARD.get();
        assertEquals("Expected another type of analyzer", StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDefaultPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DEFAULT.get();
        assertEquals("Expected another type of analyzer", StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetKeywordPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.KEYWORD.get();
        assertEquals("Expected another type of analyzer", KeywordAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetStopPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.STOP.get();
        assertEquals("Expected another type of analyzer", StopAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetWhitespacePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.WHITESPACE.get();
        assertEquals("Expected another type of analyzer", WhitespaceAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSimplePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SIMPLE.get();
        assertEquals("Expected another type of analyzer", SimpleAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetClassicPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CLASSIC.get();
        assertEquals("Expected another type of analyzer", ClassicAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetArabicPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ARABIC.get();
        assertEquals("Expected another type of analyzer", ArabicAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetArmenianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ARMENIAN.get();
        assertEquals("Expected another type of analyzer", ArmenianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBasquePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BASQUE.get();
        assertEquals("Expected another type of analyzer", BasqueAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBrazilianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BRAZILIAN.get();
        assertEquals("Expected another type of analyzer", BrazilianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBulgarianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BULGARIAN.get();
        assertEquals("Expected another type of analyzer", BulgarianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCaatalanPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CATALAN.get();
        assertEquals("Expected another type of analyzer", CatalanAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetChinesePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CHINESE.get();
        assertEquals("Expected another type of analyzer", StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCjkPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CJK.get();
        assertEquals("Expected another type of analyzer", CJKAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCzechPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CZECH.get();
        assertEquals("Expected another type of analyzer", CzechAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDutchPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DUTCH.get();
        assertEquals("Expected another type of analyzer", DutchAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDanishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DANISH.get();
        assertEquals("Expected another type of analyzer", DanishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetEnglishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ENGLISH.get();
        assertEquals("Expected another type of analyzer", EnglishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetFinnishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.FINNISH.get();
        assertEquals("Expected another type of analyzer", FinnishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetFrenchPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.FRENCH.get();
        assertEquals("Expected another type of analyzer", FrenchAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGalicianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GALICIAN.get();
        assertEquals("Expected another type of analyzer", GalicianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGermanPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GERMAN.get();
        assertEquals("Expected another type of analyzer", GermanAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGreekPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GREEK.get();
        assertEquals("Expected another type of analyzer", GreekAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetHindiPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.HINDI.get();
        assertEquals("Expected another type of analyzer", HindiAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetHungarianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.HUNGARIAN.get();
        assertEquals("Expected another type of analyzer", HungarianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetIndonesianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.INDONESIAN.get();
        assertEquals("Expected another type of analyzer", IndonesianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetIrishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.IRISH.get();
        assertEquals("Expected another type of analyzer", IrishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetItalianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ITALIAN.get();
        assertEquals("Expected another type of analyzer", ItalianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetLatvianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.LATVIAN.get();
        assertEquals("Expected another type of analyzer", LatvianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetNorwegianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.NORWEGIAN.get();
        assertEquals("Expected another type of analyzer", NorwegianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPersianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.PERSIAN.get();
        assertEquals("Expected another type of analyzer", PersianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPortuguesePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.PORTUGUESE.get();
        assertEquals("Expected another type of analyzer", PortugueseAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetRomanianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ROMANIAN.get();
        assertEquals("Expected another type of analyzer", RomanianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetRussianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.RUSSIAN.get();
        assertEquals("Expected another type of analyzer", RussianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSoraniPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SORANI.get();
        assertEquals("Expected another type of analyzer", SoraniAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSpanishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SPANISH.get();
        assertEquals("Expected another type of analyzer", SpanishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSwedishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SWEDISH.get();
        assertEquals("Expected another type of analyzer", SwedishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetTurkishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.TURKISH.get();
        assertEquals("Expected another type of analyzer", TurkishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetThaiPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.THAI.get();
        assertEquals("Expected another type of analyzer", ThaiAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerFromNameLowerCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("standard");
        assertNotNull("Expected null analyzer", analyzer);
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerFromNameUpperCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("STANDARD");
        assertNotNull("Expected null analyzer", analyzer);
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerUnexistent() {
        Analyzer analyzer = PreBuiltAnalyzers.get("unexistent");
        assertNull("Expected null analyzer", analyzer);
    }

}
