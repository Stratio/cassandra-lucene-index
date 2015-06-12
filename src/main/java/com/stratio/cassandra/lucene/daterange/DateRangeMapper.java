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
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;

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

    private final String from;
    private final String to;

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    private final DateRangePrefixTree tree;
    private final SpatialStrategy strategy;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Builds a new {@link DateRangeMapper}.
     *
     * @param name The name of the mapper.
     */
    public DateRangeMapper(String name, Boolean indexed, Boolean sorted, String from, String to, String pattern) {
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

        if (StringUtils.isBlank(from)) {
            throw new IllegalArgumentException("from column name is required");
        }

        if (StringUtils.isBlank(to)) {
            throw new IllegalArgumentException("to column name is required");
        }

        this.from = from;
        this.to = to;
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

    @Override
    public void addFields(Document document, Columns columns) {

        Date from = readFrom(columns);
        Date to = readTo(columns);

        NumberRangePrefixTree.UnitNRShape start = tree.toUnitShape(from);
        NumberRangePrefixTree.UnitNRShape stop = tree.toUnitShape(to);
        NumberRangePrefixTree.NRShape shape = tree.toRangeShape(start, stop);

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
                      .add("from", from)
                      .add("to", to)
                      .add("tree", tree)
                      .add("strategy", strategy)
                      .toString();
    }

    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, from);
        validate(metadata, to);
    }

    /**
     * Returns the longitude contained in the specified {@link Columns}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param columns The {@link Columns} containing the latitude.
     */
    Date readFrom(Columns columns) {
        Column column = columns.getColumnsByName(this.from).getFirst();
        if (column == null) {
            throw new IllegalArgumentException("from column required");
        }
        Object value = column.getComposedValue();
        Date from = base(value);
        if (from == null) {
            throw new IllegalArgumentException("Valid start date required, but found " + value);
        }
        return from;
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns The {@link Columns} containing the latitude.
     */
    Date readTo(Columns columns) {
        Column column = columns.getColumnsByName(this.to).getFirst();
        if (column == null) {
            throw new IllegalArgumentException("to column required");
        }
        Object value = column.getComposedValue();
        Date to = base(value);
        if (to == null) {
            throw new IllegalArgumentException("Valid stop date required, but found " + value);
        }
        return to;
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
        return null;
    }
}
