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
package com.stratio.cassandra.lucene.daterange;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.query.SingleFieldCondition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import java.util.Date;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeCondition extends SingleFieldCondition {

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

    /** If the lower value must be included if not null. */
    @JsonProperty("include_lower")
    private final boolean includeStart;

    /** If the upper value must be included if not null. */
    @JsonProperty("include_upper")
    private final boolean includeStop;

    /**
     * Constructs a query selecting all fields greater/equal than {@code start} but less/equal than {@code
     * stop}.
     * <p/>
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost        The boost for this query clause. Documents matching this clause will (in addition to the
     *                     normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                     #DEFAULT_BOOST} is used as default.
     * @param field        The name of the field to be matched.
     * @param start   The lower accepted value. Maybe {@code null} meaning no lower limit.
     * @param stop   The upper accepted value. Maybe {@code null} meaning no upper limit.
     * @param includeStart if {@code true}, the {@code lowerValue} is included in the range.
     * @param includeStop if {@code true}, the {@code upperValue} is included in the range.
     */
    @JsonCreator
    public DateRangeCondition(@JsonProperty("boost") Float boost,
                              @JsonProperty("field") String field,
                              @JsonProperty("start") Object start,
                              @JsonProperty("stop") Object stop,
                              @JsonProperty("include_lower") Boolean includeStart,
                              @JsonProperty("include_upper") Boolean includeStop) {
        super(boost, field);
        this.field = field;
        this.start = start;
        this.stop = stop;
        this.includeStart = includeStart == null ? DEFAULT_INCLUDE_START : includeStart;
        this.includeStop = includeStop == null ? DEFAULT_INCLUDE_STOP : includeStop;
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
     * Returns {@code true} if the {@link #stop} value is included in the range, {@code false} otherwise.
     *
     * @return {@code true} if the {@link #stop} value is included in the range, {@code false} otherwise.
     */
    public Boolean getIncludeStart() {
        return includeStart;
    }

    /**
     * Returns {@code true} if the {@link #start} value is included in the range, {@code false} otherwise.
     *
     * @return {@code true} if the {@link #start} value is included in the range, {@code false} otherwise.
     */
    public Boolean getIncludeStop() {
        return includeStop;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema) {

        ColumnMapper columnMapper = schema.getMapper(field);
        if (!(columnMapper instanceof DateRangeMapper)) {
            throw new IllegalArgumentException("Date range mapper required");
        }
        DateRangeMapper mapper = (DateRangeMapper) columnMapper;
        NumberRangePrefixTreeStrategy strategy = mapper.getStrategy();

        Date start = mapper.base(this.start);
        Date stop = mapper.base(this.stop);

        NRShape shape = mapper.makeShape(start, stop);

        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);
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
                      .add("includeStart", includeStart)
                      .add("includeStop", includeStop)
                      .toString();
    }
}