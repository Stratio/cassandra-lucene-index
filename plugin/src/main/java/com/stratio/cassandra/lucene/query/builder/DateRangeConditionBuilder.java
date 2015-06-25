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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.DateRangeCondition;
import com.stratio.cassandra.lucene.query.GeoBBoxCondition;
import com.stratio.cassandra.lucene.query.GeoDistanceCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link GeoBBoxCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeConditionBuilder extends ConditionBuilder<DateRangeCondition, DateRangeConditionBuilder> {

    @JsonProperty("field")
    private String field; // The name of the field to be matched.
    @JsonProperty("start")
    private Object start; // The lower accepted date. Maybe null meaning no lower limit.
    @JsonProperty("stop")
    private Object stop; // The upper accepted date. Maybe null meaning no upper limit.
    @JsonProperty("operation")
    private String operation; // The spatial operation to be performed.

    /**
     * Returns a new {@link DateRangeConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     */
    @JsonCreator
    public DateRangeConditionBuilder(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the lower accepted date. Maybe null meaning no lower limit.
     *
     * @param start The lower accepted date. Maybe null meaning no lower limit.
     * @return This.
     */
    public DateRangeConditionBuilder setStart(Object start) {
        this.start = start;
        return this;
    }

    /**
     * Sets the upper accepted date. Maybe null meaning no lower limit.
     *
     * @param stop The upper accepted date. Maybe null meaning no lower limit.
     * @return This.
     */
    public DateRangeConditionBuilder setStop(Object stop) {
        this.stop = stop;
        return this;
    }

    /**
     * Sets the operation to be performed.
     *
     * @param operation The operation to be performed.
     * @return This.
     */
    public DateRangeConditionBuilder setOperation(String operation) {
        this.operation = operation;
        return this;
    }

    /**
     * Returns the {@link GeoDistanceCondition} represented by this builder.
     *
     * @return The {@link GeoDistanceCondition} represented by this builder.
     */
    @Override
    public DateRangeCondition build() {
        return new DateRangeCondition(boost, field, start, stop, operation);
    }
}
