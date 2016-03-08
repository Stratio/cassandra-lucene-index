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
 * A {@link Condition} implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsCondition extends Condition {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The value of the field to be matched. */
    @JsonProperty("values")
    final Object[] values;

    /**
     * Creates a new {@link ContainsCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     */
    @JsonCreator
    public ContainsCondition(@JsonProperty("field") String field, @JsonProperty("values") Object... values) {
        this.field = field;
        this.values = values;
    }
}
