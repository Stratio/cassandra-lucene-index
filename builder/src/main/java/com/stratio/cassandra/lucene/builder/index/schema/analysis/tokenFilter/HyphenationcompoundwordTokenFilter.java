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
 public class HyphenationcompoundwordTokenFilter extends TokenFilter{

    @JsonCreator
    public HyphenationcompoundwordTokenFilter(){}

    @JsonCreator
    public HyphenationcompoundwordTokenFilter(String hyphenator, String encoding, String dictionary, Integer minWordSize, Integer minSubwordSize, Integer maxSubwordSize, Boolean onlyLongestMatch) {
        this.hyphenator = hyphenator;
        this.encoding = encoding;
        this.dictionary = dictionary;
        this.minWordSize = minWordSize;
        this.minSubwordSize = minSubwordSize;
        this.maxSubwordSize = maxSubwordSize;
        this.onlyLongestMatch = onlyLongestMatch;
    }

    private String hyphenator;
    private String encoding;
    private String dictionary;
    private Integer minWordSize;
    private Integer minSubwordSize;
    private Integer maxSubwordSize;
    private Boolean onlyLongestMatch;


    public String getHyphenator() {
        return hyphenator;
    }

    public HyphenationcompoundwordTokenFilter setHyphenator(String hyphenator) {
        this.hyphenator = hyphenator;
        return this;
    }

    public String getEncoding() {
        return encoding;
    }

    public HyphenationcompoundwordTokenFilter setEncoding(String encoding) {
        this.encoding = encoding;
        return this;
    }

    public String getDictionary() {
        return dictionary;
    }

    public HyphenationcompoundwordTokenFilter setDictionary(String dictionary) {
        this.dictionary = dictionary;
        return this;
    }

    public Integer getMinWordSize() {
        return minWordSize;
    }

    public HyphenationcompoundwordTokenFilter setMinWordSize(Integer minWordSize) {
        this.minWordSize = minWordSize;
        return this;
    }

    public Integer getMinSubwordSize() {
        return minSubwordSize;
    }

    public HyphenationcompoundwordTokenFilter setMinSubwordSize(Integer minSubwordSize) {
        this.minSubwordSize = minSubwordSize;
        return this;
    }

    public Integer getMaxSubwordSize() {
        return maxSubwordSize;
    }

    public HyphenationcompoundwordTokenFilter setMaxSubwordSize(Integer maxSubwordSize) {
        this.maxSubwordSize = maxSubwordSize;
        return this;
    }

    public Boolean getOnlyLongestMatch() {
        return onlyLongestMatch;
    }

    public HyphenationcompoundwordTokenFilter setOnlyLongestMatch(Boolean onlyLongestMatch) {
        this.onlyLongestMatch = onlyLongestMatch;
        return this;
    }
}

