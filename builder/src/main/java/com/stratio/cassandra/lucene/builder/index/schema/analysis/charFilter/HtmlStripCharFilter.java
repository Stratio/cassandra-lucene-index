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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.charFilter;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.ArrayList;

/**
 * Created by jpgilaberte on 30/05/17.
 */
public class HtmlStripCharFilter extends CharFilter{

    @JsonCreator
    public HtmlStripCharFilter(){}

    @JsonCreator
    public HtmlStripCharFilter(ArrayList<String> escapedtags) {
        this.escapedtags = escapedtags;
    }

    private ArrayList<String> escapedtags;

    public ArrayList<String> getEscapedtags() {
        return escapedtags;
    }

    public HtmlStripCharFilter setEscapedtags(ArrayList<String> escapedtags) {
        this.escapedtags = escapedtags;
        return this;
    }
}
