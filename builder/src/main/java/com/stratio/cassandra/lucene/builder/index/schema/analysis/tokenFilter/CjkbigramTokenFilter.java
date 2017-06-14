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
 public class CjkbigramTokenFilter extends TokenFilter{

    @JsonCreator
    public CjkbigramTokenFilter(){}

    @JsonCreator
    public CjkbigramTokenFilter(Boolean han, Boolean hiragana, Boolean katakana, Boolean hangul, Boolean outputUnigrams) {
        this.han = han;
        this.hiragana = hiragana;
        this.katakana = katakana;
        this.hangul = hangul;
        this.outputUnigrams = outputUnigrams;
    }

    private Boolean han;
    private Boolean hiragana;
    private Boolean katakana;
    private Boolean hangul;
    private Boolean outputUnigrams;

    public Boolean getHan() {
        return han;
    }

    public CjkbigramTokenFilter setHan(Boolean han) {
        this.han = han;
        return this;
    }

    public Boolean getHiragana() {
        return hiragana;
    }

    public CjkbigramTokenFilter setHiragana(Boolean hiragana) {
        this.hiragana = hiragana;
        return this;
    }

    public Boolean getKatakana() {
        return katakana;
    }

    public CjkbigramTokenFilter setKatakana(Boolean katakana) {
        this.katakana = katakana;
        return this;
    }

    public Boolean getHangul() {
        return hangul;
    }

    public CjkbigramTokenFilter setHangul(Boolean hangul) {
        this.hangul = hangul;
        return this;
    }

    public Boolean getOutputUnigrams() {
        return outputUnigrams;
    }

    public CjkbigramTokenFilter setOutputUnigrams(Boolean outputUnigrams) {
        this.outputUnigrams = outputUnigrams;
        return this;
    }
}

