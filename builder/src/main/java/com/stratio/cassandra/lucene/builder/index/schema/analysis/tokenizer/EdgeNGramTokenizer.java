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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link Tokenizer} using a Lucene's {@code Tokenizer}s in classpath.
 *
 * It's uses the {@code Tokenizer}'s default (no args) constructor.
 *
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
public class EdgeNGramTokenizer extends Tokenizer {
    static final Integer DEFAULT_MIN_GRAM = 1;
    static final Integer DEFAULT_MAX_GRAM = 2;

    /** the smallest n-gram to generate */
    @JsonProperty("min_gram")
    final Integer minGram;

    /** the largest n-gram to generate */
    @JsonProperty("max_gram")
    final Integer maxGram;

    /**
     * Builds a new {@link EdgeNGramTokenizer} using the default minGram and manGram.
     */
    @JsonCreator
    public EdgeNGramTokenizer() {
        this.minGram = DEFAULT_MIN_GRAM;
        this.maxGram = DEFAULT_MAX_GRAM;
    }

    /**
     * Builds a new {@link EdgeNGramTokenizer} using the specified minGram and manGram.
     *
     * @param minGram the smallest n-gram to generate
     * @param minGram the largest n-gram to generate
     */
    @JsonCreator
    public EdgeNGramTokenizer(@JsonProperty("min_gram") Integer minGram,
                              @JsonProperty("max_gram") Integer maxGram) {
        this.minGram = getOrDefault(minGram, DEFAULT_MIN_GRAM);
        this.maxGram = getOrDefault(maxGram, DEFAULT_MAX_GRAM);
    }
}
