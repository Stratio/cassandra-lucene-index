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
 public class ElisionTokenFilter extends TokenFilter{

    @JsonCreator
    public ElisionTokenFilter(){}

    @JsonCreator
    public ElisionTokenFilter(String articles, Boolean ignoreCase) {
        this.articles = articles;
        this.ignoreCase = ignoreCase;
    }

    private String articles;
    private Boolean ignoreCase;

    public String getArticles() {
        return articles;
    }

    public ElisionTokenFilter setArticles(String articles) {
        this.articles = articles;
        return this;
    }

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public ElisionTokenFilter setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
        return this;
    }
}

