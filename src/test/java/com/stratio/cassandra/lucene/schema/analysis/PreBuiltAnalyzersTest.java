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
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PreBuiltAnalyzersTest {

    @Test
    public void testGetStandardPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.STANDARD.get();
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDefaultPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DEFAULT.get();
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetKeywordPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.KEYWORD.get();
        Assert.assertEquals(KeywordAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetStopPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.STOP.get();
        Assert.assertEquals(StopAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetWhitespacePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.WHITESPACE.get();
        Assert.assertEquals(WhitespaceAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSimplePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SIMPLE.get();
        Assert.assertEquals(SimpleAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetClassicPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CLASSIC.get();
        Assert.assertEquals(ClassicAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetArabicPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ARABIC.get();
        Assert.assertEquals(ArabicAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetArmenianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ARMENIAN.get();
        Assert.assertEquals(ArmenianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBasquePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BASQUE.get();
        Assert.assertEquals(BasqueAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBrazilianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BRAZILIAN.get();
        Assert.assertEquals(BrazilianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetBulgarianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.BULGARIAN.get();
        Assert.assertEquals(BulgarianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCaatalanPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CATALAN.get();
        Assert.assertEquals(CatalanAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetChinesePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CHINESE.get();
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCjkPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CJK.get();
        Assert.assertEquals(CJKAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetCzechPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.CZECH.get();
        Assert.assertEquals(CzechAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDutchPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DUTCH.get();
        Assert.assertEquals(DutchAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetDanishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.DANISH.get();
        Assert.assertEquals(DanishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetEnglishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ENGLISH.get();
        Assert.assertEquals(EnglishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetFinnishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.FINNISH.get();
        Assert.assertEquals(FinnishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetFrenchPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.FRENCH.get();
        Assert.assertEquals(FrenchAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGalicianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GALICIAN.get();
        Assert.assertEquals(GalicianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGermanPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GERMAN.get();
        Assert.assertEquals(GermanAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetGreekPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.GREEK.get();
        Assert.assertEquals(GreekAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetHindiPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.HINDI.get();
        Assert.assertEquals(HindiAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetHungarianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.HUNGARIAN.get();
        Assert.assertEquals(HungarianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetIndonesianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.INDONESIAN.get();
        Assert.assertEquals(IndonesianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetIrishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.IRISH.get();
        Assert.assertEquals(IrishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetItalianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ITALIAN.get();
        Assert.assertEquals(ItalianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetLatvianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.LATVIAN.get();
        Assert.assertEquals(LatvianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetNorwegianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.NORWEGIAN.get();
        Assert.assertEquals(NorwegianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPersianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.PERSIAN.get();
        Assert.assertEquals(PersianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPortuguesePreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.PORTUGUESE.get();
        Assert.assertEquals(PortugueseAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetRomanianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.ROMANIAN.get();
        Assert.assertEquals(RomanianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetRussianPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.RUSSIAN.get();
        Assert.assertEquals(RussianAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSoraniPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SORANI.get();
        Assert.assertEquals(SoraniAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSpanishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SPANISH.get();
        Assert.assertEquals(SpanishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetSwedishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.SWEDISH.get();
        Assert.assertEquals(SwedishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetTurkishPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.TURKISH.get();
        Assert.assertEquals(TurkishAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetThaiPreBuiltAnalyzer() {
        Analyzer analyzer = PreBuiltAnalyzers.THAI.get();
        Assert.assertEquals(ThaiAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerFromNameLowerCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("standard");
        Assert.assertNotNull(analyzer);
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerFromNameUpperCase() {
        Analyzer analyzer = PreBuiltAnalyzers.get("STANDARD");
        Assert.assertNotNull(analyzer);
        Assert.assertEquals(StandardAnalyzer.class, analyzer.getClass());
        analyzer.close();
    }

    @Test
    public void testGetPreBuiltAnalyzerUnexistent() {
        Analyzer analyzer = PreBuiltAnalyzers.get("unexistent");
        Assert.assertNull(analyzer);
    }

}
