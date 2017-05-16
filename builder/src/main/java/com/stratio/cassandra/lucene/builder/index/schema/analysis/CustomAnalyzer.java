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
package com.stratio.cassandra.lucene.builder.index.schema.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer.Tokenizer;

/**
 * {@link Analyzer} using a Lucene's {@code Analyzer}s in classpath.
 *
 * It's uses the {@code Analyzer}'s default (no args) constructor.
 *
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
public class CustomAnalyzer extends Analyzer{

    @JsonProperty("tokenizer")
    private final Tokenizer tokenizer;

    /**
     * Builds a new {@link CustomAnalyzer} using custom tokenizer, char_filters and token_filters.
     *
     * @param tokenizer an {@link Tokenizer} the tookenizer to use.
     */
    @JsonCreator
    public CustomAnalyzer(@JsonProperty("tokenizer") Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }
}
