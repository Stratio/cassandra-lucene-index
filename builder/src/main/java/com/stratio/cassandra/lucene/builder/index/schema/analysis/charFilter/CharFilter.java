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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.stratio.cassandra.lucene.builder.JSONBuilder;

/**
 * Created by jpgilaberte on 25/05/17.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = MappingCharFilter.class, name = "mapping"),
        @JsonSubTypes.Type(value = HtmlStripCharFilter.class, name = "htmlstrip"),
        @JsonSubTypes.Type(value = PatternCharFilter.class, name = "pattern"),
        @JsonSubTypes.Type(value = PersianCharFilter.class, name = "persian")})
public class CharFilter extends JSONBuilder{

}
