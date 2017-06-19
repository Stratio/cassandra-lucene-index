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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenFilter;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Created by jpgilaberte on 25/05/17.
 */
 public class SynonymTokenFilter extends TokenFilter{

    @JsonCreator
    public SynonymTokenFilter(){}

    @JsonCreator
    public SynonymTokenFilter(String synonyms, String format, Boolean ignoreCase, Boolean expand, String tokenizerFactory) {
        this.synonyms = synonyms;
        this.format = format;
        this.ignoreCase = ignoreCase;
        this.expand = expand;
        this.tokenizerFactory = tokenizerFactory;
    }

    private String synonyms;
    private String format;
    private Boolean ignoreCase;
    private Boolean expand;
    private String tokenizerFactory;

    public String getSynonyms() {
        return synonyms;
    }

    public SynonymTokenFilter setSynonyms(String synonyms) {
        this.synonyms = synonyms;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public SynonymTokenFilter setFormat(String format) {
        this.format = format;
        return this;
    }

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public SynonymTokenFilter setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
        return this;
    }

    public Boolean getExpand() {
        return expand;
    }

    public SynonymTokenFilter setExpand(Boolean expand) {
        this.expand = expand;
        return this;
    }

    public String getTokenizerFactory() {
        return tokenizerFactory;
    }

    public SynonymTokenFilter setTokenizerFactory(String tokenizerFactory) {
        this.tokenizerFactory = tokenizerFactory;
        return this;
    }
}

