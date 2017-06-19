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
 public class LimittokencountTokenFilter extends TokenFilter{

    @JsonCreator
    public LimittokencountTokenFilter(){}

    @JsonCreator
    public LimittokencountTokenFilter(Integer maxTokenCount, Boolean consumeAllTokens) {
        this.maxTokenCount = maxTokenCount;
        this.consumeAllTokens = consumeAllTokens;
    }

    private Integer maxTokenCount;
    private Boolean consumeAllTokens;

    public Integer getMaxTokenCount() {
        return maxTokenCount;
    }

    public LimittokencountTokenFilter setMaxTokenCount(Integer maxTokenCount) {
        this.maxTokenCount = maxTokenCount;
        return this;
    }

    public Boolean getConsumeAllTokens() {
        return consumeAllTokens;
    }

    public LimittokencountTokenFilter setConsumeAllTokens(Boolean consumeAllTokens) {
        this.consumeAllTokens = consumeAllTokens;
        return this;
    }
}

