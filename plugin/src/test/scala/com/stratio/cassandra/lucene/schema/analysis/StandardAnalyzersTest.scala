/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
package com.stratio.cassandra.lucene.schema.analysis

import com.stratio.cassandra.lucene.BaseScalaTest
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.br.BrazilianAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.ckb.SoraniAnalyzer
import org.apache.lucene.analysis.core.{KeywordAnalyzer, SimpleAnalyzer, StopAnalyzer, WhitespaceAnalyzer}
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.fa.PersianAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.gl.GalicianAnalyzer
import org.apache.lucene.analysis.hi.HindiAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.hy.ArmenianAnalyzer
import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.lv.LatvianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ro.RomanianAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.standard.{ClassicAnalyzer, StandardAnalyzer}
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.th.ThaiAnalyzer
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class StandardAnalyzersTest extends BaseScalaTest {

    test("GetStandardPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.STANDARD
        assertEquals("Expected another type of analyzer", classOf[StandardAnalyzer], analyzer.getClass)
    }

    test("GetDefaultPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.DEFAULT
        assertEquals("Expected another type of analyzer", classOf[StandardAnalyzer], analyzer.getClass)
    }

    test("GetKeywordPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.KEYWORD
        assertEquals("Expected another type of analyzer", classOf[KeywordAnalyzer], analyzer.getClass)
    }

    test("GetStopPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.STOP
        assertEquals("Expected another type of analyzer", classOf[StopAnalyzer], analyzer.getClass)
    }

    test("GetWhitespacePreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.WHITESPACE
        assertEquals("Expected another type of analyzer", classOf[WhitespaceAnalyzer], analyzer.getClass)
    }

    test("GetSimplePreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.SIMPLE
        assertEquals("Expected another type of analyzer", classOf[SimpleAnalyzer], analyzer.getClass)
    }

    test("GetClassicPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.CLASSIC
        assertEquals("Expected another type of analyzer", classOf[ClassicAnalyzer], analyzer.getClass)
    }

    test("GetArabicPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.ARABIC
        assertEquals("Expected another type of analyzer", classOf[ArabicAnalyzer], analyzer.getClass)
    }

    test("GetArmenianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.ARMENIAN
        assertEquals("Expected another type of analyzer", classOf[ArmenianAnalyzer], analyzer.getClass)
    }

    test("GetBasquePreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.BASQUE
        assertEquals("Expected another type of analyzer", classOf[BasqueAnalyzer], analyzer.getClass)
    }

    test("GetBrazilianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.BRAZILIAN
        assertEquals("Expected another type of analyzer", classOf[BrazilianAnalyzer], analyzer.getClass)
    }

    test("GetBulgarianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.BULGARIAN
        assertEquals("Expected another type of analyzer", classOf[BulgarianAnalyzer], analyzer.getClass)
    }

    test("GetCatalanPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.CATALAN
        assertEquals("Expected another type of analyzer", classOf[CatalanAnalyzer], analyzer.getClass)
    }

    test("GetChinesePreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.CHINESE
        assertEquals("Expected another type of analyzer", classOf[StandardAnalyzer], analyzer.getClass)
    }

    test("GetCjkPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.CJK
        assertEquals("Expected another type of analyzer", classOf[CJKAnalyzer], analyzer.getClass)
    }

    test("GetCzechPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.CZECH
        assertEquals("Expected another type of analyzer", classOf[CzechAnalyzer], analyzer.getClass)
    }

    test("GetDutchPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.DUTCH
        assertEquals("Expected another type of analyzer", classOf[DutchAnalyzer], analyzer.getClass)
    }

    test("GetDanishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.DANISH
        assertEquals("Expected another type of analyzer", classOf[DanishAnalyzer], analyzer.getClass)
    }

    test("GetEnglishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.ENGLISH
        assertEquals("Expected another type of analyzer", classOf[EnglishAnalyzer], analyzer.getClass)
    }

    test("GetFinnishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.FINNISH
        assertEquals("Expected another type of analyzer", classOf[FinnishAnalyzer], analyzer.getClass)
    }

    test("GetFrenchPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.FRENCH
        assertEquals("Expected another type of analyzer", classOf[FrenchAnalyzer], analyzer.getClass)
    }

    test("GetGalicianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.GALICIAN
        assertEquals("Expected another type of analyzer", classOf[GalicianAnalyzer], analyzer.getClass)
    }

    test("GetGermanPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.GERMAN
        assertEquals("Expected another type of analyzer", classOf[GermanAnalyzer], analyzer.getClass)
    }

    test("GetGreekPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.GREEK
        assertEquals("Expected another type of analyzer", classOf[GreekAnalyzer], analyzer.getClass)
    }

    test("GetHindiPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.HINDI
        assertEquals("Expected another type of analyzer", classOf[HindiAnalyzer], analyzer.getClass)
    }

    test("GetHungarianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.HUNGARIAN
        assertEquals("Expected another type of analyzer", classOf[HungarianAnalyzer], analyzer.getClass)
    }

    test("GetIndonesianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.INDONESIAN
        assertEquals("Expected another type of analyzer", classOf[IndonesianAnalyzer], analyzer.getClass)
    }

    test("GetIrishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.IRISH
        assertEquals("Expected another type of analyzer", classOf[IrishAnalyzer], analyzer.getClass)
    }

    test("GetItalianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.ITALIAN
        assertEquals("Expected another type of analyzer", classOf[ItalianAnalyzer], analyzer.getClass)
    }

    test("GetLatvianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.LATVIAN
        assertEquals("Expected another type of analyzer", classOf[LatvianAnalyzer], analyzer.getClass)
    }

    test("GetNorwegianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.NORWEGIAN
        assertEquals("Expected another type of analyzer", classOf[NorwegianAnalyzer], analyzer.getClass)
    }

    test("GetPersianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.PERSIAN
        assertEquals("Expected another type of analyzer", classOf[PersianAnalyzer], analyzer.getClass)
    }

    test("GetPortuguesePreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.PORTUGUESE
        assertEquals("Expected another type of analyzer", classOf[PortugueseAnalyzer], analyzer.getClass)
    }

    test("GetRomanianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.ROMANIAN
        assertEquals("Expected another type of analyzer", classOf[RomanianAnalyzer], analyzer.getClass)
    }

    test("GetRussianPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.RUSSIAN
        assertEquals("Expected another type of analyzer", classOf[RussianAnalyzer], analyzer.getClass)
    }

    test("GetSoraniPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.SORANI
        assertEquals("Expected another type of analyzer", classOf[SoraniAnalyzer], analyzer.getClass)
    }

    test("GetSpanishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.SPANISH
        assertEquals("Expected another type of analyzer", classOf[SpanishAnalyzer], analyzer.getClass)
    }

    test("GetSwedishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.SWEDISH
        assertEquals("Expected another type of analyzer", classOf[SwedishAnalyzer], analyzer.getClass)
    }

    test("GetTurkishPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.TURKISH
        assertEquals("Expected another type of analyzer", classOf[TurkishAnalyzer], analyzer.getClass)
    }

    test("GetThaiPreBuiltAnalyzer") {
        val analyzer = StandardAnalyzers.THAI
        assertEquals("Expected another type of analyzer", classOf[ThaiAnalyzer], analyzer.getClass)
    }

    test("StandardAnalyzerFromNameLowerCase") {
        val analyzer = StandardAnalyzers.get("standard")
        assertNotNull("Expected not null analyzer", analyzer)
    }

    test("StandardAnalyzerFromNameUpperCase") {
        val analyzer = StandardAnalyzers.get("STANDARD")
        assertNotNull("Expected not null analyzer", analyzer)
    }

    test("StandardAnalyzerNonExistent") {
        val analyzer = StandardAnalyzers.get("non-existent")
        assertNull("Expected null analyzer", analyzer)
    }
}
