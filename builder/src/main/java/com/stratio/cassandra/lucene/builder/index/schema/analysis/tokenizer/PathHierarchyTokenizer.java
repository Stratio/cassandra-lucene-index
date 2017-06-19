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
public class PathHierarchyTokenizer extends Tokenizer {

    static final Boolean REVERSE = false;
    static final Character DEFAULT_DELIMITER = '/';
    static final Character DEFAULT_REPLACEMENT = '/';
    static final Integer DEFAULT_SKIP = 0;

    /** terms cache read buffer size */
    @JsonProperty("reverse")
    final Boolean reverse;

    /** path separator */
    @JsonProperty("delimiter")
    final Character delimiter;

    /** a replacement character for delimiter */
    @JsonProperty("replace")
    final Character replace;

    /** number of initial tokens to skip */
    @JsonProperty("skip")
    final Integer skip;

    /**
     * Builds a new {@link PathHierarchyTokenizer} using the specified bufferSize, delimiter, replacement and skip.
     */
    @JsonCreator
    public PathHierarchyTokenizer() {
        this.reverse = REVERSE;
        this.delimiter = DEFAULT_DELIMITER;
        this.replace = DEFAULT_REPLACEMENT;
        this.skip = DEFAULT_SKIP;
    }

    /**
     * Builds a new {@link PathHierarchyTokenizer} using the default bufferSize, delimiter, replacement and skip.
     *
     * @param reverse terms cache read buffer size
     * @param delimiter path separator
     * @param replace a replacement character for delimiter
     * @param skip number of initial tokens to skip
     */
    @JsonCreator
    public PathHierarchyTokenizer(@JsonProperty("reverse") Boolean reverse,
                                  @JsonProperty("delimiter") Character delimiter,
                                  @JsonProperty("replace") Character replace,
                                  @JsonProperty("skip") Integer skip) {
        this.reverse = getOrDefault(reverse, REVERSE);
        this.delimiter = getOrDefault(delimiter, DEFAULT_DELIMITER);
        this.replace = getOrDefault(replace, DEFAULT_REPLACEMENT);
        this.skip = getOrDefault(skip, DEFAULT_SKIP);
    }
}
