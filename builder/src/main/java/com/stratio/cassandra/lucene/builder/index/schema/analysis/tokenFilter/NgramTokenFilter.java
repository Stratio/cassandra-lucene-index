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
 public class NgramTokenFilter extends TokenFilter{

    @JsonCreator
    public NgramTokenFilter(){}

    @JsonCreator
    public NgramTokenFilter(Integer minGramSize, Integer maxGramSize) {
        this.minGramSize = minGramSize;
        this.maxGramSize = maxGramSize;
    }

    private Integer minGramSize	= 1;
    private Integer maxGramSize = 2;

    public Integer getMinGramSize() {
        return minGramSize;
    }

    public NgramTokenFilter setMinGramSize(Integer minGramSize) {
        this.minGramSize = minGramSize;
        return this;
    }

    public Integer getMaxGramSize() {
        return maxGramSize;
    }

    public NgramTokenFilter setMaxGramSize(Integer maxGramSize) {
        this.maxGramSize = maxGramSize;
        return this;
    }
}

