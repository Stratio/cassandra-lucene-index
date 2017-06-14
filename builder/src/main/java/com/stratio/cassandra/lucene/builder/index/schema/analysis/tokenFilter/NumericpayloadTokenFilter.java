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
 public class NumericpayloadTokenFilter extends TokenFilter{

    @JsonCreator
    public NumericpayloadTokenFilter(){}

    @JsonCreator
    public NumericpayloadTokenFilter(Integer payload, String typeMatch) {
        this.payload = payload;
        this.typeMatch = typeMatch;
    }

    private Integer payload;
    private String typeMatch;

    public Integer getPayload() {
        return payload;
    }

    public NumericpayloadTokenFilter setPayload(Integer payload) {
        this.payload = payload;
        return this;
    }

    public String getTypeMatch() {
        return typeMatch;
    }

    public NumericpayloadTokenFilter setTypeMatch(String typeMatch) {
        this.typeMatch = typeMatch;
        return this;
    }
}

