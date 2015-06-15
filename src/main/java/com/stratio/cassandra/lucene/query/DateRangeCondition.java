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
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Date;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeCondition extends Condition {

    /** The default include lower option. */
    public static final boolean DEFAULT_INCLUDE_START = false;

    /** The default include upper option. */
    public static final boolean DEFAULT_INCLUDE_STOP = false;

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    @JsonProperty("start")
    private final Object start;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    @JsonProperty("stop")
    private final Object stop;

    /** The spatial operation to be performed. */
    @JsonProperty("operation")
    private final SpatialOperation operation;

    /**
     * Constructs a query selecting all fields greater/equal than {@code start} but less/equal than {@code stop}.
     * <p/>
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost        The boost for this query clause. Documents matching this clause will (in addition to the
     *                     normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                     #DEFAULT_BOOST} is used as default.
     * @param field        The name of the field to be matched.
     * @param start        The lower accepted {@link Date}. Maybe {@code null} meaning no lower limit.
     * @param stop         The upper accepted {@link Date}. Maybe {@code null} meaning no upper limit.
     */
    @JsonCreator
    public DateRangeCondition(@JsonProperty("boost") Float boost,
                              @JsonProperty("field") String field,
                              @JsonProperty("start") Object start,
                              @JsonProperty("stop") Object stop,
                              @JsonProperty("operator") String operator) {
        super(boost);
        this.field = field;
        this.start = start;
        this.stop = stop;
        if (operator == null ) {
            this.operation = SpatialOperation.Intersects;
        } else if (operator.equalsIgnoreCase("is_within")) {
            this.operation = SpatialOperation.IsWithin;
        } else if (operator.equalsIgnoreCase("contains")) {
            this.operation = SpatialOperation.Contains;
        } else if (operator.equalsIgnoreCase("intersects")) {
            this.operation = SpatialOperation.Intersects;
        } else {
            throw new IllegalArgumentException("Operator is invalid: " + operator);
        }
    }

    /**
     * Returns the lower accepted value. Maybe {@code null} meaning no lower limit.
     *
     * @return The lower accepted value. Maybe {@code null} meaning no lower limit.
     */
    public Object getStart() {
        return start;
    }

    /**
     * Returns the upper accepted value. Maybe {@code null} meaning no upper limit.
     *
     * @return The upper accepted value. Maybe {@code null} meaning no upper limit.
     */
    public Object getStop() {
        return stop;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema) {

        Mapper columnMapper = schema.getMapper(field);
        if (!(columnMapper instanceof DateRangeMapper)) {
            throw new IllegalArgumentException("Date range mapper required");
        }
        DateRangeMapper mapper = (DateRangeMapper) columnMapper;
        SpatialStrategy strategy = mapper.getStrategy();

        Date start = mapper.base(this.start);
        Date stop = mapper.base(this.stop);

        NRShape shape = mapper.makeShape(start, stop);

        SpatialArgs args = new SpatialArgs(operation, shape);
        Query query = strategy.makeQuery(args);
        query.setBoost(boost);
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("boost", boost)
                      .add("field", field)
                      .add("start", start)
                      .add("stop", stop)
                      .add("operation", operation)
                      .toString();
    }
}