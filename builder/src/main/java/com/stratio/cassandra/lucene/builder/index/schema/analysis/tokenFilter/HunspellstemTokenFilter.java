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
 public class HunspellstemTokenFilter extends TokenFilter{

    @JsonCreator
    public HunspellstemTokenFilter(){}

    @JsonCreator
    public HunspellstemTokenFilter(String dictionary, String affix, boolean longestOnly) {
        this.dictionary = dictionary;
        this.affix = affix;
        this.longestOnly = longestOnly;
    }

    private String dictionary;
    private String affix;
    private boolean longestOnly;

    public String getDictionary() {
        return dictionary;
    }

    public HunspellstemTokenFilter setDictionary(String dictionary) {
        this.dictionary = dictionary;
        return this;
    }

    public String getAffix() {
        return affix;
    }

    public HunspellstemTokenFilter setAffix(String affix) {
        this.affix = affix;
        return this;
    }

    public boolean isLongestOnly() {
        return longestOnly;
    }

    public HunspellstemTokenFilter setLongestOnly(boolean longestOnly) {
        this.longestOnly = longestOnly;
        return this;
    }
}

