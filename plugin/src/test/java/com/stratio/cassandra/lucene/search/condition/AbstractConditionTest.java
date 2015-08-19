/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.util.JsonSerializer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Abstract class for {@link ConditionBuilder} tests.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class AbstractConditionTest {

    protected void testJsonSerialization(ConditionBuilder<?, ?> conditionBuilder, String json) {
        try {
            String json1 = JsonSerializer.toString(conditionBuilder);
            assertEquals("JSON serialization is wrong", json, json1);
            String json2 = JsonSerializer.toString(JsonSerializer.fromString(json1, ConditionBuilder.class));
            assertEquals("JSON serialization is wrong", json1, json2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
