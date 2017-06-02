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
public class PatternTokenizer extends Tokenizer {

    static final String DEFAULT_PATTERN = "\\W+";
    static final Integer DEFAULT_GROUP = -1;

    /** java regular expression <a href="http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html"</a> */
    @JsonProperty("pattern")
    final String pattern;

    /** which pattern group to use to generate tokens (-1 for split) */
    @JsonProperty("group")
    final Integer group;

    /**
     * Builds a new {@link PatternTokenizer} using the default pattern, flags, and group.
     */
    @JsonCreator
    public PatternTokenizer() {
        this.pattern = DEFAULT_PATTERN;
        this.group = DEFAULT_GROUP;
    }

    /**
     * Builds a new {@link PatternTokenizer} using the specified pattern, flags, and group.
     *
     * @param pattern java regular expression
     * @param flags java regular expression flags
     * @param group a pattern group to use to generate tokens (-1 for split)
     */
    @JsonCreator
    public PatternTokenizer(@JsonProperty("pattern") String pattern,
                            @JsonProperty("flags") Integer flags,
                            @JsonProperty("group") Integer group) {
        this.pattern = getOrDefault(pattern, DEFAULT_PATTERN);
        this.group = getOrDefault(group, DEFAULT_GROUP);
    }
}
