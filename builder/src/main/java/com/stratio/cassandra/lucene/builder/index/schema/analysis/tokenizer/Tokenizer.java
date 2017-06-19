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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.stratio.cassandra.lucene.builder.JSONBuilder;

/**
 * A Lucene {@code Tokenizer}.
 *
 * @author jpgilaberte@stratio.com {@literal <jpgilaberte@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ClassicTokenizer.class, name = "classic"),
               @JsonSubTypes.Type(value = EdgeNGramTokenizer.class, name = "edge_ngram"),
               @JsonSubTypes.Type(value = KeywordTokenizer.class, name = "keyword"),
               @JsonSubTypes.Type(value = LetterTokenizer.class, name = "letter"),
               @JsonSubTypes.Type(value = LowerCaseTokenizer.class, name = "lower_case"),
               @JsonSubTypes.Type(value = NGramTokenizer.class, name = "ngram"),
               @JsonSubTypes.Type(value = PathHierarchyTokenizer.class, name = "path_hierarchy"),
               @JsonSubTypes.Type(value = PatternTokenizer.class, name = "pattern"),
               @JsonSubTypes.Type(value = StandardTokenizer.class, name = "standard"),
               @JsonSubTypes.Type(value = UAX29URLEmailTokenizer.class, name = "uax29_url_email"),
               @JsonSubTypes.Type(value = ThaiTokenizer.class, name = "thai"),
               @JsonSubTypes.Type(value = WhitespaceTokenizer.class, name = "whitespace"),
               @JsonSubTypes.Type(value = WikipediaTokenizer.class, name = "wikipedia")})
public abstract class Tokenizer extends JSONBuilder {

    /**
     *
     *
     * @param param the main parameter.
     * @param defaultParam the default parameter if main paramaeter is null.
     * @param <T> return type must extend {@link Tokenizer}
     * @return if (param!=null) { return param; }else{ return defaultParam; }
     */
    public static <T> T getOrDefault(T param, T defaultParam) {
        if (param == null) {
            return defaultParam;
        } else {
            return param;
        }
    }
}
