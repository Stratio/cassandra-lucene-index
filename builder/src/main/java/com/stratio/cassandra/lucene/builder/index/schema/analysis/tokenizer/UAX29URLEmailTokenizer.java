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
public class UAX29URLEmailTokenizer extends Tokenizer {

    static final Integer DEFAULT_MAX_TOKEN_LENGTH = 255;

    /** If a token length is bigger that this, token is split at max token length intervals. */
    @JsonProperty("max_token_length")
    final Integer maxTokenLength;

    /**
     * Builds a new {@link UAX29URLEmailTokenizer} using the default maxTokenLength.
     *
     */
    @JsonCreator
    public UAX29URLEmailTokenizer() {
        this.maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

    }

    /**
     * Builds a new {@link UAX29URLEmailTokenizer} using the specified maxTokenLength.
     *
     * @param maxTokenLength if a token length is bigger that this, token is split at max token length intervals.
     */
    @JsonCreator
    public UAX29URLEmailTokenizer(@JsonProperty("max_token_length") Integer maxTokenLength) {
        this.maxTokenLength = getOrDefault(maxTokenLength, DEFAULT_MAX_TOKEN_LENGTH);

    }
}
