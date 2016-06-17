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
package com.stratio.cassandra.lucene.index;

import com.google.common.base.MoreObjects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link AnalyzerWrapper} which discards too large tokens.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TokenLengthAnalyzer extends AnalyzerWrapper {

    private static final Logger logger = LoggerFactory.getLogger(TokenLengthAnalyzer.class);

    private final Analyzer analyzer;

    /**
     * Builds a new {@link TokenLengthAnalyzer} which wraps the specified {@link Analyzer}.
     *
     * @param analyzer the {@link Analyzer} to be wrapped
     */
    public TokenLengthAnalyzer(Analyzer analyzer) {
        super(analyzer.getReuseStrategy());
        this.analyzer = analyzer;
    }

    /** {@inheritDoc} */
    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer;
    }

    /**
     * Returns the wrapped {@link Analyzer}.
     *
     * @return the wrapped {@link Analyzer}
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /** {@inheritDoc} */
    @Override
    protected TokenStreamComponents wrapComponents(final String fieldName, TokenStreamComponents components) {
        TokenFilter tokenFilter = new TokenLengthFilter(components.getTokenStream(), fieldName);
        return new TokenStreamComponents(components.getTokenizer(), tokenFilter);
    }

    /** {@link FilteringTokenFilter} that discards too large tokens. */
    private static final class TokenLengthFilter extends FilteringTokenFilter {

        private final CharTermAttribute tm = addAttribute(CharTermAttribute.class);
        private final String fieldName;

        private TokenLengthFilter(TokenStream tokenStream, String fieldName) {
            super(tokenStream);
            this.fieldName = fieldName;
        }

        /** {@inheritDoc} */
        @Override
        protected boolean accept() throws IOException {
            int maxSize = IndexWriter.MAX_TERM_LENGTH;
            int size = tm.length();
            if (size > maxSize) {
                logger.warn("Discarding immense term in field='{}', " +
                            "Lucene only allows terms with at most " +
                            "{} bytes in length; got {}", fieldName, maxSize, size);
                return false;
            }
            return true;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("analyzer", analyzer).toString();
    }
}
