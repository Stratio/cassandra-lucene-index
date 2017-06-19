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
 public class CapitalizationTokenFilter extends TokenFilter{

    @JsonCreator
    public CapitalizationTokenFilter(){}

    @JsonCreator
    public CapitalizationTokenFilter(Boolean onlyFirstWord, String keep, Boolean keepIgnoreCase, String okPrefix) {
        this.onlyFirstWord = onlyFirstWord;
        this.keep = keep;
        this.keepIgnoreCase = keepIgnoreCase;
        this.okPrefix = okPrefix;
    }

    private Boolean onlyFirstWord;
    private String keep;
    private Boolean keepIgnoreCase;
    private String okPrefix;

    public Boolean getOnlyFirstWord() {
        return onlyFirstWord;
    }

    public CapitalizationTokenFilter setOnlyFirstWord(Boolean onlyFirstWord) {
        this.onlyFirstWord = onlyFirstWord;
        return this;
    }

    public String getKeep() {
        return keep;
    }

    public CapitalizationTokenFilter setKeep(String keep) {
        this.keep = keep;
        return this;
    }

    public Boolean getKeepIgnoreCase() {
        return keepIgnoreCase;
    }

    public CapitalizationTokenFilter setKeepIgnoreCase(Boolean keepIgnoreCase) {
        this.keepIgnoreCase = keepIgnoreCase;
        return this;
    }

    public String getOkPrefix() {
        return okPrefix;
    }

    public CapitalizationTokenFilter setOkPrefix(String okPrefix) {
        this.okPrefix = okPrefix;
        return this;
    }
}

