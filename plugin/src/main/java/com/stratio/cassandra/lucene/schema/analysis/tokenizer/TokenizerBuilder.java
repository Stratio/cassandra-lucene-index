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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.lucene.analysis.Tokenizer;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ClassicTokenizerBuilder.class, name = "classic"),
               @JsonSubTypes.Type(value = EdgeNGramTokenizerBuilder.class, name = "edge_ngram"),
               @JsonSubTypes.Type(value = KeywordTokenizerBuilder.class, name = "keyword"),
               @JsonSubTypes.Type(value = LetterTokenizerBuilder.class, name = "letter"),
               @JsonSubTypes.Type(value = LowerCaseTokenizerBuilder.class, name = "lower_case"),
               @JsonSubTypes.Type(value = NGramTokenizerBuilder.class, name = "ngram"),
               @JsonSubTypes.Type(value = PathHierarchyTokenizerBuilder.class, name = "path_hierarchy"),
               @JsonSubTypes.Type(value = PatternTokenizerBuilder.class, name = "pattern"),
               @JsonSubTypes.Type(value = ReversePathHierarchyTokenizerBuilder.class, name = "reverse_path_hierarchy"),
               @JsonSubTypes.Type(value = StandardTokenizerBuilder.class, name = "standard"),
               @JsonSubTypes.Type(value = UAX29URLEmailTokenizerBuilder.class, name = "uax29_url_email"),
               @JsonSubTypes.Type(value = UnicodeWhitespaceTokenizerBuilder.class, name = "unicode_whitespace"),
               @JsonSubTypes.Type(value = ThaiTokenizerBuilder.class, name = "thai"),
               @JsonSubTypes.Type(value = WhitespaceTokenizerBuilder.class, name = "whitespace"),
               @JsonSubTypes.Type(value = WikipediaTokenizerBuilder.class, name = "wikipedia")})
public abstract class TokenizerBuilder<T extends Tokenizer> {

    /**
     * Gets or creates the Lucene {@link Tokenizer}.
     *
     * @return the built analyzer
     */
    public abstract T buildTokenizer();

    /**
     * @param param the main parameter.
     * @param defaultParam the default parameter if main paramaeter is null.
     * @param <T> return type must extend {@link Tokenizer}
     * @return if (param!=null) { return param; }else{ return defaultParam; }
     */
    public static <T> T getOrDefault(T param, T defaultParam) {
        if (param==null) {
            return defaultParam;
        } else {
            return param;
        }
    }
}
