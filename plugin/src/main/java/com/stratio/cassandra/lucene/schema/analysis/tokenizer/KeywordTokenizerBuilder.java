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
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.core.KeywordTokenizer}
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class KeywordTokenizerBuilder extends TokenizerBuilder<KeywordTokenizer> {

    static final Integer DEFAULT_BUFFER_SIZE = 256;

    /** terms cache read buffer size */
    @JsonProperty("buffer_size")
    final Integer bufferSize;

    /**
     * Builds a new {@link KeywordTokenizerBuilder} using the specified buffer_size.
     *
     * @param bufferSize the terms cache read buffer size
     */
    @JsonCreator
    public KeywordTokenizerBuilder(@JsonProperty("buffer_size") Integer bufferSize) {
        this.bufferSize = getOrDefault(bufferSize, DEFAULT_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override
    public KeywordTokenizer buildTokenizer() {
        return new KeywordTokenizer(bufferSize);
    }
}
