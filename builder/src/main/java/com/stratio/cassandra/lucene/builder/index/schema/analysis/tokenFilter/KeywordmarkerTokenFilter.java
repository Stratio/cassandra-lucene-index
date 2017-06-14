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
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jpgilaberte on 25/05/17.
 */
 public class KeywordmarkerTokenFilter extends TokenFilter{

    @JsonCreator
    public KeywordmarkerTokenFilter(){}

    @JsonCreator
    public KeywordmarkerTokenFilter(String protect, String pattern, Boolean ignoreCase) {
        this.protect = protect;
        this.pattern = pattern;
        this.ignoreCase = ignoreCase;
    }

    @JsonProperty("protected")
    private String protect;
    private String pattern;
    private Boolean ignoreCase;

    public String getProtect() {
        return protect;
    }

    public KeywordmarkerTokenFilter setProtect(String protect) {
        this.protect = protect;
        return this;
    }

    public String getPattern() {
        return pattern;
    }

    public KeywordmarkerTokenFilter setPattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public KeywordmarkerTokenFilter setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
        return this;
    }
}

