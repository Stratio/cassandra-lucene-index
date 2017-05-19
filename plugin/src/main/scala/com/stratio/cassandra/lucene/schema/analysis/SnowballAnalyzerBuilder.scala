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

import java.util

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.IndexException
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.{LowerCaseFilter, StopFilter}
import org.apache.lucene.analysis.standard.{StandardFilter, StandardTokenizer}
import org.apache.lucene.analysis.util.CharArraySet

/**
 * [[AnalyzerBuilder]] for tartarus.org snowball [[Analyzer]].
 *
 * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
 * Norwegian, Danish, Russian, Finnish, Hungarian and Turkish.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
  * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
 * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Hungarian and Turkish. Basque and
 * Catalan.
 * @param stopwords the comma separated stopwords list.
 */
class SnowballAnalyzerBuilder @JsonCreator() (  @JsonProperty("language") language: String,
                                                @JsonProperty("stopwords") stopwords: String ) extends AnalyzerBuilder {

    // Check language
    if (StringUtils.isBlank(language)) throw new IndexException("Language must be specified")

    /** @inheritdoc*/
    override def analyzer(): Analyzer = {
        // Setup stopwords
        val stops = if (stopwords == null) SnowballAnalyzerBuilder.getDefaultStopwords(language) else SnowballAnalyzerBuilder.getStopwords(stopwords)
        SnowballAnalyzerBuilder.buildAnalyzer(language, stops)
    }
}

object SnowballAnalyzerBuilder {
    /**
     * Returns the snowball [[org.apache.lucene.analysis.Analyzer]] for the specified language and stopwords.
     *
     * @param language The language code. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @param stopwords the stop words list
     * @return a new snowball analyzer
     */
    private def buildAnalyzer(language: String, stopwords: CharArraySet) :  Analyzer = new SnowballAnalyzer(language, stopwords)

    /**
     * Returns the stopwords [[CharArraySet]] for the specified comma separated stopwords [[String]].
     *
     * @param stopwords a [[String]] comma separated stopwords list
     * @return the stopwords list as a char array set
     */
    private def getStopwords(stopwords: String ) :  CharArraySet = {
        val stopwordsList = new util.ArrayList[String]()
        for (stop : String <- stopwords.split(",")) {
            stopwordsList.add(stop.trim)
        }
        new CharArraySet(stopwordsList, true)
    }

    /**
     * Returns the default stopwords set used by Lucene language analyzer for the specified language.
     *
     * @param language The language for which the stopwords are. The supported languages are English, French, Spanish,
     * Portuguese, Italian, Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     * Turkish, Armenian, Basque and Catalan.
     * @return the default stopwords set used by Lucene language analyzers
     */
    private def getDefaultStopwords(language: String) : CharArraySet = StandardStopwords.get(language)

    /**
     * A tartarus.org snowball [[Analyzer]].
     * Builds a new [[SnowballAnalyzer]] for the specified language and stopwords.
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @param stopwords the comma separated stopwords [[String]]
     */
    class SnowballAnalyzer(language : String, stopwords : CharArraySet) extends Analyzer {

        /** @inheritdoc */
        override def createComponents(fieldName: String ) : Analyzer.TokenStreamComponents = {
            val source = new StandardTokenizer()
            var result : TokenStream = new StandardFilter(source).asInstanceOf[TokenStream]
            result = new LowerCaseFilter(result)
            result = new StopFilter(result, stopwords)
            result = new SnowballFilter(result, language)
            new TokenStreamComponents(source, result)
        }
    }
}
