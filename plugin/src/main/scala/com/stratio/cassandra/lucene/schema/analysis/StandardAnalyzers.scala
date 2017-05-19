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

import org.apache.lucene.analysis.Analyzer
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

/**
 * Prebuilt Lucene [[Analyzer]]s that can be instantiated by name.
 */
object StandardAnalyzers extends Enumeration {
    type StandardAnalyzers = Value
    val ARABIC = Value("arabic")
    val ARMENIAN = Value("armenian")
    val BASQUE = Value("basque")
    val BRAZILIAN = Value("brazilian")
    val BULGARIAN = Value("bulgarian")
    val CATALAN = Value("catalan")
    val CHINESE = Value("chinese")
    val CJK = Value("cjk")
    val CLASSIC = Value("classic")
    val CZECH = Value("czech")
    val DANISH = Value("danish")
    val DEFAULT = Value("default")
    val DUTCH = Value("dutch")
    val ENGLISH = Value("english")
    val FINNISH = Value("finnish")
    val FRENCH = Value("french")
    val GALICIAN = Value("galician")
    val GERMAN = Value("german")
    val GREEK = Value("greek")
    val HINDI = Value("hindi")
    val HUNGARIAN = Value("hungarian")
    val INDONESIAN = Value("indonesian")
    val IRISH = Value("irish")
    val ITALIAN = Value("italian")
    val KEYWORD = Value("keyword")
    val LATVIAN = Value("latvian")
    val NORWEGIAN = Value("norwegian")
    val PERSIAN = Value("persian")
    val PORTUGUESE = Value("portuguese")
    val ROMANIAN = Value("romanian")
    val RUSSIAN = Value("russian")
    val SIMPLE = Value("simple")
    val SORANI = Value("sorani")
    val SPANISH = Value("spanish")
    val STANDARD = Value("standard")
    val STOP = Value("stop")
    val SWEDISH = Value("swedish")
    val THAI = Value("thai")
    val TURKISH = Value("turkish")
    val WHITESPACE = Value("whitespace")

    /**
      * Returns the prebuilt [[Analyzer]] identified by the specified name, or {{{null}}} if there is no such
      * [[Analyzer]].
      *
      * @param name a prebuilt [[Analyzer]] name
      * @return the analyzer, or {{{null}}} if there is no such analyzer
      */
    def get(name: String): Analyzer = {

        val standardAnalyzer = Value(name.toLowerCase)
        standardAnalyzer match {
            case ARABIC => new ArabicAnalyzer()
            case ARMENIAN => new ArmenianAnalyzer()
            case BASQUE => new BasqueAnalyzer()
            case BRAZILIAN => new BrazilianAnalyzer()
            case BULGARIAN => new BulgarianAnalyzer()
            case CATALAN => new CatalanAnalyzer()
            case CHINESE => new StandardAnalyzer()
            case CJK => new CJKAnalyzer()
            case CLASSIC => new ClassicAnalyzer()
            case CZECH => new CzechAnalyzer()
            case DANISH => new DanishAnalyzer()
            case DEFAULT => new StandardAnalyzer()
            case DUTCH => new DutchAnalyzer()
            case ENGLISH => new EnglishAnalyzer()
            case FINNISH => new FinnishAnalyzer()
            case FRENCH => new FrenchAnalyzer()
            case GALICIAN => new GalicianAnalyzer()
            case GERMAN => new GermanAnalyzer()
            case GREEK => new GreekAnalyzer()
            case HINDI => new HindiAnalyzer()
            case HUNGARIAN => new HungarianAnalyzer()
            case INDONESIAN => new IndonesianAnalyzer()
            case IRISH => new IrishAnalyzer()
            case ITALIAN => new ItalianAnalyzer()
            case KEYWORD => new KeywordAnalyzer()
            case LATVIAN => new LatvianAnalyzer()
            case NORWEGIAN => new NorwegianAnalyzer()
            case PERSIAN => new PersianAnalyzer()
            case PORTUGUESE => new PortugueseAnalyzer()
            case ROMANIAN => new RomanianAnalyzer()
            case RUSSIAN => new RussianAnalyzer()
            case SIMPLE => new SimpleAnalyzer()
            case SORANI => new SoraniAnalyzer()
            case SPANISH => new SpanishAnalyzer()
            case STANDARD => new StandardAnalyzer()
            case STOP => new StopAnalyzer()
            case SWEDISH => new SwedishAnalyzer()
            case THAI => new ThaiAnalyzer()
            case TURKISH => new TurkishAnalyzer()
            case WHITESPACE => new WhitespaceAnalyzer()
            case _ => null
        }
    }
}