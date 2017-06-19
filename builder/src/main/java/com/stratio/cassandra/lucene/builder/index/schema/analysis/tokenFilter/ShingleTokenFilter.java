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
 public class ShingleTokenFilter extends TokenFilter{

    @JsonCreator
    public ShingleTokenFilter(){}

    @JsonCreator
    public ShingleTokenFilter(Integer min_shingle_size, Integer max_shingle_size, Boolean outputUnigrams, Boolean OUIfNoShingles, String tokenSeparator, String fillerToken) {
        this.min_shingle_size = min_shingle_size;
        this.max_shingle_size = max_shingle_size;
        this.outputUnigrams = outputUnigrams;
        this.OUIfNoShingles = OUIfNoShingles;
        this.tokenSeparator = tokenSeparator;
        this.fillerToken = fillerToken;
    }

    private Integer min_shingle_size = 2;
    private Integer max_shingle_size = 2;
    private Boolean outputUnigrams = false;
    private Boolean OUIfNoShingles = false;
    private String tokenSeparator;
    private String fillerToken;

    public Integer getMin_shingle_size() {
        return min_shingle_size;
    }

    public ShingleTokenFilter setMin_shingle_size(Integer min_shingle_size) {
        this.min_shingle_size = min_shingle_size;
        return this;
    }

    public Integer getMax_shingle_size() {
        return max_shingle_size;
    }

    public ShingleTokenFilter setMax_shingle_size(Integer max_shingle_size) {
        this.max_shingle_size = max_shingle_size;
        return this;
    }

    public Boolean getOutputUnigrams() {
        return outputUnigrams;
    }

    public ShingleTokenFilter setOutputUnigrams(Boolean outputUnigrams) {
        this.outputUnigrams = outputUnigrams;
        return this;
    }

    public Boolean getOUIfNoShingles() {
        return OUIfNoShingles;
    }

    public ShingleTokenFilter setOUIfNoShingles(Boolean OUIfNoShingles) {
        this.OUIfNoShingles = OUIfNoShingles;
        return this;
    }

    public String getTokenSeparator() {
        return tokenSeparator;
    }

    public ShingleTokenFilter setTokenSeparator(String tokenSeparator) {
        this.tokenSeparator = tokenSeparator;
        return this;
    }

    public String getFillerToken() {
        return fillerToken;
    }

    public ShingleTokenFilter setFillerToken(String fillerToken) {
        this.fillerToken = fillerToken;
        return this;
    }
}

