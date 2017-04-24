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
package com.stratio.cassandra.lucene.schema.analysis.tokenizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

/**
 * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.ngram.NGramTokenizer}
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class NGramTokenizerBuilder extends TokenizerBuilder<NGramTokenizer> {

    static final Integer DEFAULT_MIN_GRAM = 1;
    static final Integer DEFAULT_MAX_GRAM = 2;

    /** the smallest n-gram to generate */
    @JsonProperty("min_gram")
    final Integer minGram;

    /** the largest n-gram to generate */
    @JsonProperty("max_gram")
    final Integer maxGram;

    /**
     * Builds a new {@link NGramTokenizerBuilder} using the specified minGram and manGram.
     *
     * @param minGram the smallest n-gram to generate
     * @param minGram the largest n-gram to generate
     */
    @JsonCreator
    public NGramTokenizerBuilder(@JsonProperty("min_gram") Integer minGram,
                                 @JsonProperty("max_gram") Integer maxGram) {
        this.minGram = getOrDefault(minGram, DEFAULT_MIN_GRAM);
        this.maxGram = getOrDefault(maxGram, DEFAULT_MAX_GRAM);
    }

    /** {@inheritDoc} */
    @Override
    public NGramTokenizer buildTokenizer() {
        return new NGramTokenizer(minGram, maxGram);
    }
}
