/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.hy.ArmenianAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import org.apache.lucene.analysis.util.CharArraySet
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class StandardStopwordsTest extends BaseScalaTest {

    test("GetArmenianPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", ArmenianAnalyzer.getDefaultStopSet, StandardStopwords.ARMENIAN)
    }

    test("GetBasquePreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", BasqueAnalyzer.getDefaultStopSet, StandardStopwords.BASQUE)
    }

    test("GetCatalanPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", CatalanAnalyzer.getDefaultStopSet, StandardStopwords.CATALAN)
    }

    test("GetDutchPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", DutchAnalyzer.getDefaultStopSet, StandardStopwords.DUTCH)
    }

    test("GetDanishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", DanishAnalyzer.getDefaultStopSet, StandardStopwords.DANISH)
    }

    test("GetEnglishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", EnglishAnalyzer.getDefaultStopSet, StandardStopwords.ENGLISH)
    }

    test("GetFinnishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", FinnishAnalyzer.getDefaultStopSet, StandardStopwords.FINNISH)
    }

    test("GetFrenchPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", FrenchAnalyzer.getDefaultStopSet, StandardStopwords.FRENCH)
    }

    test("GetGermanPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", GermanAnalyzer.getDefaultStopSet, StandardStopwords.GERMAN)
    }

    test("GetHungarianPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", HungarianAnalyzer.getDefaultStopSet, StandardStopwords.HUNGARIAN)
    }

    test("GetIrishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", IrishAnalyzer.getDefaultStopSet, StandardStopwords.IRISH)
    }

    test("GetItalianPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", ItalianAnalyzer.getDefaultStopSet, StandardStopwords.ITALIAN)
    }

    test("GetNorwegianPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", NorwegianAnalyzer.getDefaultStopSet, StandardStopwords.NORWEGIAN)
    }

    test("GetPortuguesePreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", PortugueseAnalyzer.getDefaultStopSet, StandardStopwords.PORTUGUESE)
    }

    test("GetRussianPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", RussianAnalyzer.getDefaultStopSet, StandardStopwords.RUSSIAN)
    }

    test("GetSpanishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", SpanishAnalyzer.getDefaultStopSet, StandardStopwords.SPANISH)
    }

    test("GetSwedishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", SwedishAnalyzer.getDefaultStopSet, StandardStopwords.SWEDISH)
    }

    test("GetTurkishPreBuiltAnalyzer") {
        assertEquals("Expected another stopwords", TurkishAnalyzer.getDefaultStopSet, StandardStopwords.TURKISH)
    }

    test("GetStandardStopwordsFromNameLowerCase") {
        assertEquals("Expected not null stopwords", EnglishAnalyzer.getDefaultStopSet, StandardStopwords.get("english"))
    }

    test("GetStandardStopwordsFromNameUpperCase") {
        assertEquals("Expected not null stopwords", EnglishAnalyzer.getDefaultStopSet, StandardStopwords.get("English"))
    }

    test("GetStandardStopwordsNonExistent") {
        assertEquals("Expected null stopwords", CharArraySet.EMPTY_SET, StandardStopwords.get("non-existent"))
    }
}
