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

import java.util.Collections;
import java.util.Set;

/**
 * {@link Tokenizer} using a Lucene's {@code Tokenizer}s in classpath.
 *
 * It's uses the {@code Tokenizer}'s default (no args) constructor.
 *
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
public class WikipediaTokenizer extends Tokenizer {

    @JsonCreator
    public WikipediaTokenizer() {}

// TODO: refactor wikipedia factory with advanced parameters
//    static final TokenOutputValue DEFAULT_TOKEN_OUTPUT = TokenOutputValue.TOKENS_ONLY;
//    static final Set<String> DEFAULT_UNTOKENIZED_TYPES = Collections.emptySet();
//
//    static final int TOKENS_ONLY_VAR = 0;
//    static final int UNTOKENIZED_ONLY_VAR = 1;
//    static final int BOTH_VAR = 2;
//
//    public enum TokenOutputValue {
//
//        TOKENS_ONLY("TOKENS_ONLY", TOKENS_ONLY_VAR),
//        UNTOKENIZED_ONLY("UNTOKENIZED_ONLY", UNTOKENIZED_ONLY_VAR),
//        BOTH("BOTH", BOTH_VAR);
//
//        private int integerValue;
//        private String stringValue;
//
//        TokenOutputValue(String name, int value) {
//            this.stringValue = name;
//            this.integerValue = value;
//        }
//
//        @JsonCreator
//        public static TokenOutputValue create(String value) {
//            if (value == null) {
//                throw new IllegalArgumentException();
//            }
//            for (TokenOutputValue v : values()) {
//                if (v.getStringValue().equals(value)) {
//                    return v;
//                }
//            }
//            throw new IllegalArgumentException();
//        }
//
//        public int getIntegerValue() {
//            return integerValue;
//        }
//
//        public String getStringValue() {
//            return stringValue;
//        }
//    }
//
//    /** this tokenizer output, only untokenized, only tokens or both */
//    @JsonProperty("token_output")
//    final TokenOutputValue tokenOutput;
//    /** //TODO */
//    @JsonProperty("untokenized_types")
//    final Set<String> untokenizedTypes;
//
//    /**
//     * Builds a new {@link WikipediaTokenizer} using the default tokenOutput and untokenizedTypes.
//     *
//     */
//    @JsonCreator
//    public WikipediaTokenizer() {
//        this.tokenOutput = DEFAULT_TOKEN_OUTPUT;
//        this.untokenizedTypes = DEFAULT_UNTOKENIZED_TYPES;
//    }
//
//    /**
//     * Builds a new {@link WikipediaTokenizer} using the specified tokenOutput and untokenizedTypes.
//     *
//     * @param tokenOutput this tokenizer output, only untokenized, only tokens or both
//     * @param untokenizedTypes //TODO
//     */
//    @JsonCreator
//    public WikipediaTokenizer(@JsonProperty("token_output") WikipediaTokenizer.TokenOutputValue tokenOutput,
//                              @JsonProperty("untokenized_types") Set<String> untokenizedTypes) {
//        this.tokenOutput = getOrDefault(tokenOutput, DEFAULT_TOKEN_OUTPUT);
//        this.untokenizedTypes = getOrDefault(untokenizedTypes, DEFAULT_UNTOKENIZED_TYPES);
//    }
}