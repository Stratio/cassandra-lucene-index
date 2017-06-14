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
 public class WorddelimiterTokenFilter extends TokenFilter{

    @JsonCreator
    public WorddelimiterTokenFilter(){}

    @JsonCreator
    public WorddelimiterTokenFilter(String protect, Integer preserveOriginal, Integer splitOnNumerics, Integer splitOnCaseChange, Integer catenateWords, Integer catenateNumbers, Integer catenateAll, Integer generateWordParts, Integer genNumberParts, Integer stemEnglishPosse, String types) {
        this.protect = protect;
        this.preserveOriginal = preserveOriginal;
        this.splitOnNumerics = splitOnNumerics;
        this.splitOnCaseChange = splitOnCaseChange;
        this.catenateWords = catenateWords;
        this.catenateNumbers = catenateNumbers;
        this.catenateAll = catenateAll;
        this.generateWordParts = generateWordParts;
        this.genNumberParts = genNumberParts;
        this.stemEnglishPosse = stemEnglishPosse;
        this.types = types;
    }

    @JsonProperty("protected")
    private String protect;
    private Integer preserveOriginal;
    private Integer splitOnNumerics = 1;
    private Integer splitOnCaseChange = 1;
    private Integer catenateWords = 0;
    private Integer catenateNumbers = 0;
    private Integer catenateAll = 0;
    private Integer generateWordParts = 1;
    private Integer genNumberParts = 1;
    private Integer stemEnglishPosse = 1;
    private String types;

    public String getProtect() {
        return protect;
    }

    public WorddelimiterTokenFilter setProtect(String protect) {
        this.protect = protect;
        return this;
    }

    public String getTypes() {
        return types;
    }

    public WorddelimiterTokenFilter setTypes(String types) {
        this.types = types;
        return this;
    }

    public Integer getGenNumberParts() {
        return genNumberParts;
    }

    public WorddelimiterTokenFilter setGenNumberParts(Integer genNumberParts) {
        this.genNumberParts = genNumberParts;
        return this;
    }

    public Integer getStemEnglishPosse() {
        return stemEnglishPosse;
    }

    public WorddelimiterTokenFilter setStemEnglishPosse(Integer stemEnglishPosse) {
        this.stemEnglishPosse = stemEnglishPosse;
        return this;
    }

    public Integer getCatenateNumbers() {
        return catenateNumbers;
    }

    public WorddelimiterTokenFilter setCatenateNumbers(Integer catenateNumbers) {
        this.catenateNumbers = catenateNumbers;
        return this;
    }

    public Integer getCatenateAll() {
        return catenateAll;
    }

    public WorddelimiterTokenFilter setCatenateAll(Integer catenateAll) {
        this.catenateAll = catenateAll;
        return this;
    }

    public Integer getGenerateWordParts() {
        return generateWordParts;
    }

    public WorddelimiterTokenFilter setGenerateWordParts(Integer generateWordParts) {
        this.generateWordParts = generateWordParts;
        return this;
    }

    public Integer getPreserveOriginal() {
        return preserveOriginal;
    }

    public WorddelimiterTokenFilter setPreserveOriginal(Integer preserveOriginal) {
        this.preserveOriginal = preserveOriginal;
        return this;
    }

    public Integer getSplitOnNumerics() {
        return splitOnNumerics;
    }

    public WorddelimiterTokenFilter setSplitOnNumerics(Integer splitOnNumerics) {
        this.splitOnNumerics = splitOnNumerics;
        return this;
    }

    public Integer getSplitOnCaseChange() {
        return splitOnCaseChange;
    }

    public WorddelimiterTokenFilter setSplitOnCaseChange(Integer splitOnCaseChange) {
        this.splitOnCaseChange = splitOnCaseChange;
        return this;
    }

    public Integer getCatenateWords() {
        return catenateWords;
    }

    public WorddelimiterTokenFilter setCatenateWords(Integer catenateWords) {
        this.catenateWords = catenateWords;
        return this;
    }
}

