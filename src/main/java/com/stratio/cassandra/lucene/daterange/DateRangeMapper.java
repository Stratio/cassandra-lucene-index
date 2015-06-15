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
package com.stratio.cassandra.lucene.daterange;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A {@link ColumnMapper} to map 1-dimensional ranges of dates.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeMapper extends ColumnMapper {

    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

    private final String start;
    private final String stop;

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    private final DateRangePrefixTree tree;
    private final NumberRangePrefixTreeStrategy strategy;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Builds a new {@link DateRangeMapper}.
     *
     * @param name The name of the mapper.
     */
    public DateRangeMapper(String name, Boolean indexed, Boolean sorted, String start, String stop, String pattern) {
        super(name,
              indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              DecimalType.instance);

        if (StringUtils.isBlank(start)) {
            throw new IllegalArgumentException("start column name is required");
        }

        if (StringUtils.isBlank(stop)) {
            throw new IllegalArgumentException("stop column name is required");
        }

        this.start = start;
        this.stop = stop;
        this.tree = DateRangePrefixTree.INSTANCE;
        this.strategy = new NumberRangePrefixTreeStrategy(tree, name);
        this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;

        concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(DateRangeMapper.this.pattern);
            }
        };
    }

    public NumberRangePrefixTreeStrategy getStrategy() {
        return strategy;
    }

    @Override
    public void addFields(Document document, Columns columns) {
        Date start = readStart(columns);
        Date stop = readStop(columns);
        NRShape shape = makeShape(start, stop);
        for (IndexableField field : strategy.createIndexableFields(shape)) {
            document.add(field);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(boolean reverse) {
        return new SortField(name, Type.LONG, reverse);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("start", start)
                      .add("stop", stop)
                      .add("tree", tree)
                      .add("strategy", strategy)
                      .toString();
    }

    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, start);
        validate(metadata, stop);
    }

    public NRShape makeShape(Date start, Date stop) {
        UnitNRShape startShape = tree.toUnitShape(start);
        UnitNRShape stopShape = tree.toUnitShape(stop);
        return tree.toRangeShape(startShape, stopShape);
    }

    /**
     * Returns the start date contained in the specified {@link Columns}.
     *
     * @param columns The {@link Columns} containing the start date.
     */
    Date readStart(Columns columns) {
        Column column = columns.getColumnsByName(start).getFirst();
        if (column == null) throw new IllegalArgumentException("Start column required");
        Date start = base(column.getComposedValue());
        if (stop == null) throw new IllegalArgumentException("Start date required");
        return start;
    }

    /**
     * Returns the stop date contained in the specified {@link Columns}.
     */
    Date readStop(Columns columns) {
        Column column = columns.getColumnsByName(stop).getFirst();
        if (column == null) throw new IllegalArgumentException("Stop column required");
        Date stop = base(column.getComposedValue());
        if (stop == null) throw new IllegalArgumentException("Stop date required");
        return stop;
    }

    public Date base(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        } else if (value instanceof String) {
            try {
                return concurrentDateFormat.get().parse(value.toString());
            } catch (ParseException e) {
                // Ignore to fail below
            }
        }
        throw new IllegalArgumentException(String.format("Valid date required, but found '%s'", value));
    }
}
