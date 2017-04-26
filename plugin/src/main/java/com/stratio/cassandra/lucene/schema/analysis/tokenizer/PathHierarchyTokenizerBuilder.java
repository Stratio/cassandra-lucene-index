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
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;

/**
 * A {@link TokenizerBuilder} for building {@link org.apache.lucene.analysis.path.PathHierarchyTokenizer}
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class PathHierarchyTokenizerBuilder extends TokenizerBuilder<PathHierarchyTokenizer> {

    static final Integer DEFAULT_BUFFER_SIZE = 1024;
    static final Character DEFAULT_DELIMITER = '/';
    static final Character DEFAULT_REPLACEMENT = '/';
    static final Integer DEFAULT_SKIP = 0;

    /** terms cache read buffer size */
    @JsonProperty("buffer_size")
    final Integer bufferSize;

    /** path separator */
    @JsonProperty("delimiter")
    final Character delimiter;

    /** a replacement character for delimiter */
    @JsonProperty("replacement")
    final Character replacement;

    /** number of initial tokens to skip */
    @JsonProperty("skip")
    final Integer skip;

    /**
     * Builds a new {@link PathHierarchyTokenizerBuilder} using the specified bufferSize, delimiter, replacement and
     * skip.
     *
     * @param bufferSize terms cache read buffer size
     * @param delimiter path separator
     * @param replacement a replacement character for delimiter
     * @param skip number of initial tokens to skip
     */
    @JsonCreator
    public PathHierarchyTokenizerBuilder(@JsonProperty("buffer_size") Integer bufferSize,
                                         @JsonProperty("delimiter") Character delimiter,
                                         @JsonProperty("replacement") Character replacement,
                                         @JsonProperty("skip") Integer skip) {
        this.bufferSize = getOrDefault(bufferSize, DEFAULT_BUFFER_SIZE);
        this.delimiter = getOrDefault(delimiter, DEFAULT_DELIMITER);
        this.replacement = getOrDefault(replacement, DEFAULT_REPLACEMENT);
        this.skip = getOrDefault(skip, DEFAULT_SKIP);
    }

    /** {@inheritDoc} */
    @Override
    public PathHierarchyTokenizer buildTokenizer() {
        return new PathHierarchyTokenizer(bufferSize, delimiter, replacement, skip);
    }
}
