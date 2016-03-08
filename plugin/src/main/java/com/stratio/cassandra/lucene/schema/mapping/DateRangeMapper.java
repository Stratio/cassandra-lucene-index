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

package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.util.Arrays;
import java.util.Date;

/**
 * A {@link Mapper} to map 1-dimensional date ranges.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapper extends Mapper {

    /** The name of the column containing the from date. */
    public final String from;

    /** The name of the column containing the to date. */
    public final String to;

    /** The date format pattern. */
    public final String pattern;

    /** The {@link DateParser}. */
    private final DateParser dateParser;

    private final DateRangePrefixTree tree;

    /** The {@link NumberRangePrefixTreeStrategy}. */
    public final NumberRangePrefixTreeStrategy strategy;

    /**
     * Builds a new {@link DateRangeMapper}.
     *
     * @param field the name of the field
     * @param validated if the field must be validated
     * @param from the name of the column containing the from date
     * @param to the name of the column containing the to date
     * @param pattern the date format pattern
     */
    public DateRangeMapper(String field, Boolean validated, String from, String to, String pattern) {
        super(field,
              true,
              false,
              validated,
              null,
              Arrays.asList(from, to),
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              SimpleDateType.instance,
              TimestampType.instance,
              TimeUUIDType.instance);

        if (StringUtils.isBlank(from)) {
            throw new IndexException("from column name is required");
        }

        if (StringUtils.isBlank(to)) {
            throw new IndexException("to column name is required");
        }

        this.from = from;
        this.to = to;
        this.tree = DateRangePrefixTree.INSTANCE;
        this.strategy = new NumberRangePrefixTreeStrategy(tree, field);
        this.pattern = pattern == null ? DateParser.DEFAULT_PATTERN : pattern;
        this.dateParser = new DateParser(this.pattern);
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {

        Date fromDate = readFrom(columns);
        Date toDate = readTo(columns);

        if (fromDate == null && toDate == null) {
            return;
        }

        validate(fromDate, toDate);

        NRShape shape = makeShape(fromDate, toDate);
        for (IndexableField field : strategy.createIndexableFields(shape)) {
            document.add(field);
        }
    }

    private void validate(Date from, Date to) {
        if (from == null) {
            throw new IndexException("From column required");
        }
        if (to == null) {
            throw new IndexException("To column required");
        }
        if (from.after(to)) {
            throw new IndexException("From:'%s' is after To:'%s'", dateParser.toString(to), dateParser.toString(from));
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("Date range mapper '%s' does not support sorting", name);
    }

    /**
     * Makes an spatial shape representing the time range defined by the two specified dates.
     *
     * @param from the start {@link Date}
     * @param to the end {@link Date}
     * @return a shape
     */
    public NRShape makeShape(Date from, Date to) {
        UnitNRShape fromShape = tree.toUnitShape(from);
        UnitNRShape toShape = tree.toUnitShape(to);
        return tree.toRangeShape(fromShape, toShape);
    }

    /**
     * Returns the start {@link Date} contained in the specified {@link Columns}.
     *
     * @param columns the columns containing the start {@link Date}
     * @return the start date
     */
    Date readFrom(Columns columns) {
        Column<?> column = columns.getColumnsByFullName(from).getFirst();
        if (column == null) {
            return null;
        }
        Date fromDate = base(column.getComposedValue());
        if (to == null) {
            throw new IndexException("From date required");
        }
        return fromDate;
    }

    /**
     * Returns the end {@link Date} contained in the specified {@link Columns}.
     *
     * @param columns the columns containing the end {@link Date}
     * @return the end date
     */
    Date readTo(Columns columns) {
        Column<?> column = columns.getColumnsByFullName(to).getFirst();
        if (column == null) {
            return null;
        }
        Date toDate = base(column.getComposedValue());
        if (toDate == null) {
            throw new IndexException("To date required");
        }
        return toDate;
    }

    /**
     * Returns the {@link Date} represented by the specified object, or {@code null} if there is no one. A {@link
     * IllegalArgumentException} if the date is not parseable.
     *
     * @param value a value which could represent a {@link Date}
     * @return the date represented by the specified object, or {@code null} if there is no one
     */
    public Date base(Object value) {
        return dateParser.parse(value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("validated", validated)
                          .add("from", from)
                          .add("to", to)
                          .add("pattern", pattern)
                          .toString();
    }
}
