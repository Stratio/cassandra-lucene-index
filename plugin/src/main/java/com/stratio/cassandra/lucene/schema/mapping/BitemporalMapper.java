/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.common.DateParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;

import java.util.*;

/**
 * A {@link Mapper} to map bitemporal DateRanges.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapper extends Mapper {

    /** The lucene Field suffix for vt_from */
    public static final String VT_FROM_FIELD_SUFFIX = ".vtFrom";

    /** The lucene Field suffix for vt_to */
    public static final String VT_TO_FIELD_SUFFIX = ".vtTo";

    /** The lucene Field suffix for tt_from */
    public static final String TT_FROM_FIELD_SUFFIX = ".ttFrom";

    /** The lucene Field suffix for tt_to */
    public static final String TT_TO_FIELD_SUFFIX = ".ttTo";

    /** The name of the column containing the valid time start. **/
    final String vtFrom;

    /** The name of the column containing the valid time stop. **/
    final String vtTo;

    /** The name of the column containing the transaction time start. **/
    final String ttFrom;

    /** The name of the column containing the transaction time stop. **/
    final String ttTo;

    /** The NOW Value. **/
    final Long nowValue;

    /** The {@link DateParser}. */
    public final DateParser parser;

    /**
     * Builds a new {@link BitemporalMapper}.
     *
     * @param field the name of the field
     * @param validated if the field must be validated
     * @param vtFrom the name of the column containing the valid time start
     * @param vtTo the name of the column containing the valid time end
     * @param ttFrom the name of the column containing the transaction time start
     * @param ttTo the name of the column containing the transaction time end
     * @param pattern the date pattern
     * @param nowValue the value representing now
     */
    public BitemporalMapper(String field,
                            Boolean validated,
                            String vtFrom,
                            String vtTo,
                            String ttFrom,
                            String ttTo,
                            String pattern,
                            Object nowValue) {

        super(field, false, validated, null, Arrays.asList(vtFrom, vtTo, ttFrom, ttTo), DATE_TYPES);

        if (StringUtils.isBlank(vtFrom)) {
            throw new IndexException("vt_from column name is required");
        }

        if (StringUtils.isBlank(vtTo)) {
            throw new IndexException("vt_to column name is required");
        }

        if (StringUtils.isBlank(ttFrom)) {
            throw new IndexException("tt_from column name is required");
        }

        if (StringUtils.isBlank(ttTo)) {
            throw new IndexException("tt_to column name is required");
        }

        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
        this.parser = new DateParser(pattern);

        // Validate pattern
        this.nowValue = (nowValue == null) ? Long.MAX_VALUE : parser.parse(nowValue).getTime();
    }

    /** {@inheritDoc} */
    @Override
    public List<IndexableField> indexableFields(Columns columns) {

        BitemporalDateTime vtFromTime = readBitemporalDate(columns, vtFrom);
        BitemporalDateTime vtToTime = readBitemporalDate(columns, vtTo);
        BitemporalDateTime ttFromTime = readBitemporalDate(columns, ttFrom);
        BitemporalDateTime ttToTime = readBitemporalDate(columns, ttTo);

        if (vtFromTime == null && vtToTime == null && ttFromTime == null && ttToTime == null) {
            return Collections.emptyList();
        }

        validate(vtFromTime, vtToTime, ttFromTime, ttToTime);

        List<IndexableField> fields = new ArrayList<>(4);
        fields.add(new LongField(field + VT_FROM_FIELD_SUFFIX, vtFromTime.toTimestamp(), STORE));
        fields.add(new LongField(field + VT_TO_FIELD_SUFFIX, vtToTime.toTimestamp(), STORE));
        fields.add(new LongField(field + TT_FROM_FIELD_SUFFIX, ttFromTime.toTimestamp(), STORE));
        fields.add(new LongField(field + TT_TO_FIELD_SUFFIX, ttToTime.toTimestamp(), STORE));
        return fields;
    }

    private void validate(BitemporalDateTime vtFrom,
                          BitemporalDateTime vtTo,
                          BitemporalDateTime ttFrom,
                          BitemporalDateTime ttTo) {
        if (vtFrom == null) {
            throw new IndexException("vt_from column required");
        }
        if (vtTo == null) {
            throw new IndexException("vt_to column required");
        }
        if (ttFrom == null) {
            throw new IndexException("tt_from column required");
        }
        if (ttTo == null) {
            throw new IndexException("tt_to column required");
        }
        if (vtFrom.after(vtTo)) {
            throw new IndexException("vt_from:'{}' is after vt_to:'{}'",
                                     vtTo.toString(parser),
                                     vtFrom.toString(parser));
        }
        if (ttFrom.after(ttTo)) {
            throw new IndexException("tt_from:'{}' is after tt_to:'{}'",
                                     ttTo.toString(parser),
                                     ttFrom.toString(parser));
        }
    }

    /**
     * Returns a {@link BitemporalDateTime} read from the specified {@link Columns}.
     *
     * @param columns the column where the data is
     * @param field the name of the field to be read from {@code columns}
     * @return a bitemporal date time
     */
    BitemporalDateTime readBitemporalDate(Columns columns, String field) {
        return parseBitemporalDate(columns.valueForField(field));
    }

    /**
     * Parses an {@link Object} into a {@link BitemporalDateTime}. It parses {@link Long} and {@link String} format
     * values based in pattern.
     *
     * @param value the object to be parsed
     * @return a bitemporal date time
     */
    public BitemporalDateTime parseBitemporalDate(Object value) {
        Date date = parser.parse(value);
        return date == null ? null : checkIfNow(date.getTime());
    }

    private BitemporalDateTime checkIfNow(Long in) {
        if (in > nowValue) {
            throw new IndexException("BitemporalDateTime value '{}' exceeds Max Value: '{}'", in, nowValue);
        } else if (in < nowValue) {
            return new BitemporalDateTime(in);
        } else {
            return new BitemporalDateTime(Long.MAX_VALUE);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("Bitemporal mapper '{}' does not support sorting", name);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("validated", validated)
                          .add("vtFrom", vtFrom)
                          .add("vtTo", vtTo)
                          .add("ttFrom", ttFrom)
                          .add("ttTo", ttTo)
                          .add("pattern", parser.pattern)
                          .add("nowValue", nowValue)
                          .toString();
    }

    public static class BitemporalDateTime implements Comparable<BitemporalDateTime> {

        public static final BitemporalDateTime MAX = new BitemporalDateTime(Long.MAX_VALUE);
        public static final BitemporalDateTime MIN = new BitemporalDateTime(0L);

        private final Long timestamp;
        private final Date date;

        /**
         * @param date A date.
         */
        BitemporalDateTime(Date date) {
            timestamp = date.getTime();
            this.date = date;
        }

        /**
         * @param timestamp A timestamp.
         */
        public BitemporalDateTime(Long timestamp) {
            if (timestamp < 0L) {
                throw new IndexException("Cannot build a BitemporalDateTime with a negative unix time");
            }
            this.timestamp = timestamp;
            date = new Date(timestamp);
        }

        public boolean isNow() {
            return timestamp.equals(MAX.timestamp);
        }

        public boolean isMax() {
            return timestamp.equals(MAX.timestamp);
        }

        public boolean isMin() {
            return timestamp.equals(0L);
        }

        public Date toDate() {
            return date;
        }

        public Long toTimestamp() {
            return timestamp;
        }

        public boolean after(BitemporalDateTime time) {
            return date.after(time.date);
        }

        @Override
        public int compareTo(BitemporalDateTime other) {
            return timestamp.compareTo(other.timestamp);
        }

        public static BitemporalDateTime max(BitemporalDateTime bt1, BitemporalDateTime bt2) {
            int result = bt1.compareTo(bt2);
            return (result <= 0) ? bt2 : bt1;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return timestamp.toString();
        }

        public String toString(DateParser dateParser) {
            return dateParser.toString(date);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BitemporalDateTime that = (BitemporalDateTime) o;
            return timestamp.equals(that.timestamp);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return timestamp.hashCode();
        }
    }
}
