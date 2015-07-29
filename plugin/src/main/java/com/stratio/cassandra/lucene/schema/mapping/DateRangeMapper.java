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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * A {@link Mapper} to map 1-dimensional ranges of dates.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapper extends Mapper {

    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

    private final String start;
    private final String stop;

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    /** The {@link DateParser} */
    private final DateParser dateParser;

    private final DateRangePrefixTree tree;
    private final NumberRangePrefixTreeStrategy strategy;

    /**
     * Builds a new {@link DateRangeMapper}.
     *
     * @param name    The name of the mapper.
     * @param start   The column containing the start {@link Date}.
     * @param stop    The column containing the stop {@link Date}.
     * @param pattern The {@link SimpleDateFormat} pattern to be used.
     */
    public DateRangeMapper(String name, String start, String stop, String pattern) {
        super(name,
              true,
              false,
              Arrays.<AbstractType>asList(AsciiType.instance,
                                          UTF8Type.instance,
                                          Int32Type.instance,
                                          LongType.instance,
                                          IntegerType.instance,
                                          FloatType.instance,
                                          DoubleType.instance,
                                          DecimalType.instance,
                                          TimestampType.instance),
              Arrays.asList(start, stop));

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
        this.dateParser = new DateParser(this.pattern);
    }

    public String getStart() {
        return start;
    }

    public String getStop() {
        return stop;
    }

    public String getPattern() {
        return pattern;
    }

    /** {@inheritDoc} */
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
    public SortField sortField(String name, boolean reverse) {
        throw new UnsupportedOperationException(String.format("Date range mapper '%s' does not support sorting", name));
    }

    /**
     * Returns the used {@link SpatialStrategy}.
     *
     * @return The used {@link SpatialStrategy}.
     */
    public SpatialStrategy getStrategy() {
        return strategy;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("start", start)
                      .add("stop", stop)
                      .add("pattern", pattern)
                      .toString();
    }

    /** {@inheritDoc} */
    @Override
    public void validate(CFMetaData metadata) {
        validate(metadata, start);
        validate(metadata, stop);
    }

    /**
     * Makes an spatial shape representing the time range defined by the two specified dates.
     *
     * @param start The start {@link Date}.
     * @param stop  The stop {@link Date}.
     * @return The spatial shape representing the time range defined by the two specified dates.
     */
    public NRShape makeShape(Date start, Date stop) {
        UnitNRShape startShape = tree.toUnitShape(start);
        UnitNRShape stopShape = tree.toUnitShape(stop);
        return tree.toRangeShape(startShape, stopShape);
    }

    /**
     * Returns the start {@link Date} contained in the specified {@link Columns}.
     *
     * @param columns The {@link Columns} containing the start {@link Date}.
     * @return The star {@link Date} contained in the specified {@link Columns}.
     */
    Date readStart(Columns columns) {
        Column column = columns.getColumnsByName(start).getFirst();
        if (column == null) throw new IllegalArgumentException("Start column required");
        Date start = base(column.getComposedValue());
        if (stop == null) throw new IllegalArgumentException("Start date required");
        return start;
    }

    /**
     * Returns the stop {@link Date} contained in the specified {@link Columns}.
     *
     * @param columns The {@link Columns} containing the stop {@link Date}.
     * @return The stop {@link Date} contained in the specified {@link Columns}.
     */
    Date readStop(Columns columns) {
        Column column = columns.getColumnsByName(stop).getFirst();
        if (column == null) throw new IllegalArgumentException("Stop column required");
        Date stop = base(column.getComposedValue());
        if (stop == null) throw new IllegalArgumentException("Stop date required");
        return stop;
    }

    /**
     * Returns the {@link Date} represented by the specified object, or {@code null} if there is no one. A {@link
     * IllegalArgumentException} if the date is not parsable.
     *
     * @param value A value which could represent a {@link Date}.
     * @return The {@link Date} represented by the specified object, or {@code null} if there is no one.
     */
    public Date base(Object value) {
        Date opt = this.dateParser.parse(value);
        if (opt == null) {
            return null;
        } else {
            return opt;
        }
    }
}
