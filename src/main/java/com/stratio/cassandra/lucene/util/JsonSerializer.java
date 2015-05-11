/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;

/**
 * A JSON mapper based on {@link org.codehaus.jackson} annotations.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class JsonSerializer {

    /** The embedded JSON serializer. */
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    // Setup serialization options
    static {
        jsonMapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // jsonMapper.setPropertyNamingStrategy(new LowerCaseWithUnderscoresStrategy());
        // jsonMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        jsonMapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
        // jsonMapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, true);
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    /**
     * Returns the JSON {@code String} representation of the specified object.
     *
     * @param value A object to be serialized.
     * @return The JSON {@code String} representation of the specified object.
     * @throws IOException If there are serialization problems.
     */
    public static String toString(Object value) throws IOException {
        return jsonMapper.writeValueAsString(value);
    }

    /**
     * Returns the object of the specified class represented by the specified JSON {@code String}.
     *
     * @param value     A JSON {@code String} to be parsed.
     * @param valueType The class of the object to be parsed.
     * @param <T>       The type of the object to be parsed.
     * @return The object of the specified class represented by the specified JSON {@code String}.
     * @throws IOException If there are parsing problems.
     */
    public static <T> T fromString(String value, Class<T> valueType) throws IOException {
        return jsonMapper.readValue(value, valueType);
    }
}
