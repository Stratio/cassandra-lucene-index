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

package com.stratio.cassandra.lucene.builder.search.condition;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Implements the wildcard search query. Supported wildcards are {@code *}, which matches any character sequence
 * (including the empty one), and {@code ?}, which matches any single character. '\' is the escape character.
 *
 * Note this query can be slow, as it needs to iterate over many terms. In order to prevent extremely slow
 * WildcardQueries, a Wildcard term should not start with the wildcard {@code *}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class WildcardCondition extends Condition {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The wildcard expression to be matched. */
    @JsonProperty("value")
    final String value;

    /**
     * Creates a new {@link WildcardCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the wildcard expression to be matched
     */
    @JsonCreator
    public WildcardCondition(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }
}
