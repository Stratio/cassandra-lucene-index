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
package com.stratio.cassandra.lucene.builder;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract builder for creating JSON documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class JSONBuilder {

    /** The embedded JSON serializer. */
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.configure(DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return build();
    }

    /**
     * Returns the JSON {@code String} representation of the specified object.
     *
     * @return a JSON {@code String}
     */
    public String build() {
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            throw new BuilderException(e, "Error formatting JSON");
        }
    }

    @SafeVarargs
    protected static <T> List<T> add(List<T> l, T... a) {
        if (l == null) {
            l = new ArrayList<>(a.length);
        }
        l.addAll(Arrays.asList(a));
        return l;
    }
}
