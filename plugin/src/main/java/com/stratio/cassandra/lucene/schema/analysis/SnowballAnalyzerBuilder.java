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

import com.stratio.cassandra.lucene.IndexException;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link AnalyzerBuilder} for tartarus.org snowball {@link Analyzer}.
 *
 * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
 * Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SnowballAnalyzerBuilder extends AnalyzerBuilder {

    @JsonProperty("language")
    private final String language;

    @JsonProperty("stopwords")
    private final String stopwords;

    /**
     * Builds a new {@link SnowballAnalyzerBuilder} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @param stopwords the comma separated stopwords list.
     */
    @JsonCreator
    public SnowballAnalyzerBuilder(@JsonProperty("language") String language,
                                   @JsonProperty("stopwords") String stopwords) {

        // Check language
        if (StringUtils.isBlank(language)) {
            throw new IndexException("Language must be specified");
        }

        this.language = language;
        this.stopwords = stopwords;
    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        // Setup stopwords
        CharArraySet stops = stopwords == null ? getDefaultStopwords(language) : getStopwords(stopwords);
        return buildAnalyzer(language, stops);
    }

    /**
     * Returns the snowball {@link Analyzer} for the specified language and stopwords.
     *
     * @param language The language code. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @param stopwords the stop words list
     * @return a new snowball analyzer
     */
    private static Analyzer buildAnalyzer(final String language, final CharArraySet stopwords) {
        return new SnowballAnalyzer(language, stopwords);
    }

    /**
     * Returns the stopwords {@link CharArraySet} for the specified comma separated stopwords {@code String}.
     *
     * @param stopwords a {@code String} comma separated stopwords list
     * @return the stopwords list as a char array set
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
     * Portuguese, Italian, Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian,
     * Turkish, Armenian, Basque and Catalan.
     * @return the default stopwords set used by Lucene language analyzers
     */
    private static CharArraySet getDefaultStopwords(String language) {
        return StandardStopwords.get(language);
    }

    /**
     * A tartarus.org snowball {@link Analyzer}.
     */
    public static class SnowballAnalyzer extends Analyzer {

        private final String language;
        private final CharArraySet stopwords;

        /**
         * Builds a new {@link SnowballAnalyzer} for the specified language and stopwords.
         *
         * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
         * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
         * Basque and Catalan.
         * @param stopwords the comma separated stopwords {@code String}
         */
        public SnowballAnalyzer(String language, CharArraySet stopwords) {
            this.language = language;
            this.stopwords = stopwords;
        }

        /** {@inheritDoc} */
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer source = new StandardTokenizer();
            TokenStream result = new StandardFilter(source);
            result = new LowerCaseFilter(result);
            result = new StopFilter(result, stopwords);
            result = new SnowballFilter(result, language);
            return new TokenStreamComponents(source, result);
        }
    }
}
