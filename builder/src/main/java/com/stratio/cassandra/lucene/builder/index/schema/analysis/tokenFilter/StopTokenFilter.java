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
 public class StopTokenFilter extends TokenFilter{

    @JsonCreator
    public StopTokenFilter(){}

    @JsonCreator
    public StopTokenFilter(String words, String format, Boolean ignore_case) {
        this.words = words;
        this.format = format;
        this.ignore_case = ignore_case;
    }

    private String words;
    private String format;
    private Boolean ignore_case;

    public String getWords() {
        return words;
    }

    public StopTokenFilter setWords(String words) {
        this.words = words;
        return this;
    }

    public Boolean getIgnore_case() {
        return ignore_case;
    }

    public StopTokenFilter setIgnore_case(Boolean ignore_case) {
        this.ignore_case = ignore_case;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public StopTokenFilter setFormat(String format) {
        this.format = format;
        return this;
    }
}

