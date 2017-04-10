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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * {@link AnalyzerBuilder} for building {@link Analyzer}s based on an advanced configuration.
 */
public class ComplexAnalyzerBuilder extends AnalyzerBuilder {
    /**
     * The tokenizer to use to build this analyzer.
     */
    @JsonProperty("tokenizer")
    private final ClassFactoryBuilder tokenizer;

    /**
     * The token streams to use, potentially wrapping each others. Use any placeholder in parameters to replace the previous instance.
     */
    @JsonProperty("token_streams")
    private final List<ClassFactoryBuilder> tokenStreams;

    @JsonCreator
    public ComplexAnalyzerBuilder(@JsonProperty("tokenizer") ClassFactoryBuilder className,
                                  @JsonProperty("token_streams") List<ClassFactoryBuilder> tokenStreams) {
        this.tokenizer = className;
        this.tokenStreams = tokenStreams;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Analyzer analyzer() {
        try {
            final Tokenizer tokenizer = this.tokenizer.build(Tokenizer.class, null);
            TokenStream tokenStream = tokenizer;
            if (tokenStreams != null) {
                Collections.reverse(tokenStreams);
                for (final ClassFactoryBuilder builder : tokenStreams) {
                    final TokenStream previous = tokenStream;
                    tokenStream = builder.build(TokenStream.class, type -> {
                        if (type == TokenStream.class) {
                            return previous;
                        }
                        return null;
                    });
                }
            }
            return new ComplexAnalyzer(tokenizer, tokenStream);
        } catch (final Exception e) {
            throw new IndexException(e);
        }
    }

    public static class ComplexAnalyzer extends Analyzer {
        private final Tokenizer tokenizer;
        private final TokenStream stream;

        private ComplexAnalyzer(final Tokenizer tokenizer, final TokenStream tokenStream) {
            this.tokenizer = tokenizer;
            this.stream = tokenStream;
        }

        @Override
        protected TokenStreamComponents createComponents(final String s) {
            return new TokenStreamComponents(tokenizer, stream);
        }
    }
}
