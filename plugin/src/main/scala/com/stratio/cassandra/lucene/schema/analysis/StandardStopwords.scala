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

import java.util.Locale;

/**
 * Prebuilt Lucene analyzer stopwords that can be instantiated by language name.
 */
object StandardStopwords extends Enumeration {
    type StandardAnalyzers = Value

    val ARMENIAN = Value("armenian")
    val BASQUE = Value("basque")
    val CATALAN = Value("catalan")
    val DANISH = Value("danish")
    val DUTCH = Value("dutch")
    val ENGLISH = Value("english")
    val FINNISH = Value("finnish")
    val FRENCH = Value("french")
    val GERMAN = Value("german")
    val HUNGARIAN = Value("hungarian")
    val IRISH = Value("irish")
    val ITALIAN = Value("italian")
    val NORWEGIAN = Value("norwegian")
    val PORTUGUESE = Value("portuguese")
    val RUSSIAN = Value("russian")
    val SPANISH = Value("spanish")
    val SWEDISH = Value("swedish")
    val TURKISH = Value("turkish")

    /**
     * Returns the prebuilt analyzer stopwords list identified by the specified name, or [[null} if there is no
     * such stopwords list.
     *
     * @param name the name of the searched analyzer
     * @return the prebuilt analyzer stopwords list identified by the specified name, or [[null} if there is no
     * such stopwords list
     */
    def get(name: String): CharArraySet = {
        val stopWord = Value(name.toLowerCase)
        stopWord match {
            case ARMENIAN => ArmenianAnalyzer.getDefaultStopSet
            case BASQUE => BasqueAnalyzer.getDefaultStopSet
            case CATALAN => CatalanAnalyzer.getDefaultStopSet
            case DANISH => DanishAnalyzer.getDefaultStopSet
            case DUTCH => DutchAnalyzer.getDefaultStopSet
            case ENGLISH => EnglishAnalyzer.getDefaultStopSet
            case FINNISH => FinnishAnalyzer.getDefaultStopSet
            case FRENCH => FrenchAnalyzer.getDefaultStopSet
            case GERMAN => GermanAnalyzer.getDefaultStopSet
            case HUNGARIAN => HungarianAnalyzer.getDefaultStopSet
            case IRISH => IrishAnalyzer.getDefaultStopSet
            case ITALIAN => ItalianAnalyzer.getDefaultStopSet
            case NORWEGIAN => NorwegianAnalyzer.getDefaultStopSet
            case PORTUGUESE => PortugueseAnalyzer.getDefaultStopSet
            case RUSSIAN => RussianAnalyzer.getDefaultStopSet
            case SPANISH => SpanishAnalyzer.getDefaultStopSet
            case SWEDISH => SwedishAnalyzer.getDefaultStopSet
            case TURKISH => TurkishAnalyzer.getDefaultStopSet
            case _ => CharArraySet.EMPTY_SET
        }
    }
}
