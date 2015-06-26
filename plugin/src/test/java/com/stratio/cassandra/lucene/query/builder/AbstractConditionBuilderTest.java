/*
 * Copyright 2015, Stratio.
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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.util.JsonSerializer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class AbstractConditionBuilderTest {

    protected void testJsonSerialization(ConditionBuilder conditionBuilder, String json) {
        try {
            String json1 = JsonSerializer.toString(conditionBuilder);
            assertEquals(json, json1);
            String json2 = JsonSerializer.toString(JsonSerializer.fromString(json1, ConditionBuilder.class));
            assertEquals(json1, json2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
