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
import org.apache.lucene.analysis.pattern.PatternTokenizer;

import java.util.regex.Pattern;

/**
 * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.pattern.PatternTokenizer}
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class PatternTokenizerBuilder extends TokenizerBuilder<PatternTokenizer> {

    static final String DEFAULT_PATTERN = "\\W+";
    static final Integer DEFAULT_FLAGS = 0;
    static final Integer DEFAULT_GROUP = -1;

    /** java regular expression <a href="http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html"</a> */
    @JsonProperty("pattern")
    final String pattern;

    /** java regular expression flags */
    @JsonProperty("flags")
    final Integer flags;

    /** which pattern group to use to generate tokens (-1 for split) */
    @JsonProperty("group")
    final Integer group;

    /**
     * Builds a new {@link PatternTokenizerBuilder} using the specified pattern, flags, and group.
     *
     * @param pattern java regular expression
     * @param flags java regular expression flags
     * @param group a pattern group to use to generate tokens (-1 for split)
     */
    @JsonCreator
    public PatternTokenizerBuilder(@JsonProperty("pattern") String pattern,
                                   @JsonProperty("flags") Integer flags,
                                   @JsonProperty("group") Integer group) {
        this.pattern = getOrDefault(pattern, DEFAULT_PATTERN);
        this.flags = getOrDefault(flags, DEFAULT_FLAGS);
        this.group = getOrDefault(group, DEFAULT_GROUP);
    }

    /** {@inheritDoc} */
    @Override
    public PatternTokenizer buildTokenizer() {
        return new PatternTokenizer(Pattern.compile(pattern, flags), group);
    }
}
