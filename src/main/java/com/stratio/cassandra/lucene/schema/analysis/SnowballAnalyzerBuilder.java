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

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link AnalyzerBuilder} for tartarus.org snowball {@link Analyzer}.
 * <p/>
 * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
 * Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SnowballAnalyzerBuilder extends AnalyzerBuilder {

    private final Analyzer analyzer;

    /**
     * Builds a new {@link SnowballAnalyzerBuilder} for the specified language and stopwords.
     *
     * @param language  The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     *                  Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     *                  Turkish, Armenian, Basque and Catalan.
     * @param stopwords The comma separated stopwords {@code String}.
     */
    @JsonCreator
    public SnowballAnalyzerBuilder(@JsonProperty("language") final String language,
                                   @JsonProperty("stopwords") String stopwords) {

        // Check language
        if (language == null || StringUtils.isBlank(language)) {
            throw new IllegalArgumentException("Language must be specified");
        }

        // Setup stopwords
        CharArraySet stops = stopwords == null ? getDefaultStopwords(language) : getStopwords(stopwords);

        // Setup analyzer
        this.analyzer = buildAnalyzer(language, stops);

        // Force analysis validation
        AnalysisUtils.instance.analyze("test", analyzer);
    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        return analyzer;
    }

    /**
     * Returns the snowball {@link Analyzer} for the specified language and stopwords.
     *
     * @param language  The language code. The supported languages are English, French, Spanish, Portuguese, Italian,
     *                  Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     *                  Turkish, Armenian, Basque and Catalan.
     * @param stopwords The stop words.
     * @return The snowball {@link Analyzer} for the specified language and stopwords.
     */
    private static Analyzer buildAnalyzer(final String language, final CharArraySet stopwords) {
        return new Analyzer() {
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new StandardTokenizer();
                TokenStream result = new StandardFilter(source);
                result = new LowerCaseFilter(result);
                result = new StopFilter(result, stopwords);
                result = new SnowballFilter(result, language);
                return new TokenStreamComponents(source, result);
            }
        };
    }

    /**
     * Returns the stopwords {@link CharArraySet} for the specified comma separated stopwords {@code String}.
     *
     * @param stopwords A {@code String} comma separated stopwords list.
     * @return The stopwords {@link CharArraySet} for the specified comma separated stopwords {@code String}.
     */
    private static CharArraySet getStopwords(String stopwords) {
        List<String> stopwordsList = new ArrayList<>();
        for (String stop : stopwords.split(",")) {
            stopwordsList.add(stop.trim());
        }
        return new CharArraySet(stopwordsList, true);
    }

    /**
     * Returns the default stopwords set used by Lucene language analyzer for the specified language.
     *
     * @param language The language for which the stopwords are. The supported languages are English, French, Spanish,
     *                 Portuguese, Italian, Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish,
     *                 Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
     * @return The default stopwords set used by Lucene language analyzers.
     */
    private static CharArraySet getDefaultStopwords(String language) {
        switch (language) {
            case "English":
                return EnglishAnalyzer.getDefaultStopSet();
            case "French":
                return FrenchAnalyzer.getDefaultStopSet();
            case "Spanish":
                return SpanishAnalyzer.getDefaultStopSet();
            case "Portuguese":
                return PortugueseAnalyzer.getDefaultStopSet();
            case "Italian":
                return ItalianAnalyzer.getDefaultStopSet();
            case "Romanian":
                return RomanianAnalyzer.getDefaultStopSet();
            case "German":
                return GermanAnalyzer.getDefaultStopSet();
            case "Dutch":
                return DutchAnalyzer.getDefaultStopSet();
            case "Swedish":
                return SwedishAnalyzer.getDefaultStopSet();
            case "Norwegian":
                return NorwegianAnalyzer.getDefaultStopSet();
            case "Danish":
                return DanishAnalyzer.getDefaultStopSet();
            case "Russian":
                return RussianAnalyzer.getDefaultStopSet();
            case "Finnish":
                return FinnishAnalyzer.getDefaultStopSet();
            case "Irish":
                return IrishAnalyzer.getDefaultStopSet();
            case "Hungarian":
                return HungarianAnalyzer.getDefaultStopSet();
            case "Turkish":
                return SpanishAnalyzer.getDefaultStopSet();
            case "Armenian":
                return SpanishAnalyzer.getDefaultStopSet();
            case "Basque":
                return BasqueAnalyzer.getDefaultStopSet();
            case "Catalan":
                return CatalanAnalyzer.getDefaultStopSet();
            default:
                return CharArraySet.EMPTY_SET;
        }
    }
}
