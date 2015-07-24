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
import com.spatial4j.core.shape.Shape;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A {@link Mapper} to map bitemporal DateRanges.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapper extends Mapper {

    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    /** Filed names for the four fields. */
    private final String vtFrom;
    private final String vtTo;
    private final String ttFrom;
    private final String ttTo;

    private BitemporalDateTime nowBitemporalDateTime = new BitemporalDateTime(Long.MAX_VALUE);

    // ttTo=now vtTo=now 2 daterangePrefixTree
    private NumberRangePrefixTreeStrategy strategy_t1_V;
    private DateRangePrefixTree tree_t1_V;
    private NumberRangePrefixTreeStrategy strategy_t1_T;
    private DateRangePrefixTree tree_t1_T;

    private NumberRangePrefixTreeStrategy strategy_t2_V;
    private DateRangePrefixTree tree_t2_V;
    private NumberRangePrefixTreeStrategy strategy_t2_T;
    private DateRangePrefixTree tree_t2_T;

    private NumberRangePrefixTreeStrategy strategy_t3_V;
    private DateRangePrefixTree tree_t3_V;
    private NumberRangePrefixTreeStrategy strategy_t3_T;
    private DateRangePrefixTree tree_t3_T;

    private NumberRangePrefixTreeStrategy strategy_t4_V;
    private DateRangePrefixTree tree_t4_V;
    private NumberRangePrefixTreeStrategy strategy_t4_T;
    private DateRangePrefixTree tree_t4_T;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Builds a new {@link BitemporalMapper}.
     *
     * @param name    the name of the mapper.
     * @param vtFrom  The column name containing the Start Valid Time.
     * @param vtTo    The column name containing the End Valid Time.
     * @param ttFrom  The column name containing the Start Transaction Time.
     * @param ttTo    The column name containing the End Transaction Time.
     * @param pattern The {@link SimpleDateFormat} pattern to be used.
     */
    public BitemporalMapper(String name,
                            String vtFrom,
                            String vtTo,
                            String ttFrom,
                            String ttTo,
                            String pattern,
                            Object nowValue) {

        super(name,
              true,
              false,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              DecimalType.instance,
              TimestampType.instance);

        if (StringUtils.isBlank(vtFrom)) {
            throw new IllegalArgumentException("vtFrom column name is required");
        }

        if (StringUtils.isBlank(vtTo)) {
            throw new IllegalArgumentException("vtTo column name is required");
        }

        if (StringUtils.isBlank(ttFrom)) {
            throw new IllegalArgumentException("ttFrom column name is required");
        }

        if (StringUtils.isBlank(ttTo)) {
            throw new IllegalArgumentException("ttTo column name is required");
        }

        this.pattern = (pattern == null) ? DEFAULT_PATTERN : pattern;
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;

        // Validate pattern
        new SimpleDateFormat(this.pattern);

        // ttTo=now vtTo=now 2 DateRangePrefixTree

        this.tree_t1_V = DateRangePrefixTree.INSTANCE;
        this.strategy_t1_V = new NumberRangePrefixTreeStrategy(tree_t1_V, name + ".t1_v");
        this.tree_t1_T = DateRangePrefixTree.INSTANCE;
        this.strategy_t1_T = new NumberRangePrefixTreeStrategy(tree_t1_T, name + ".t1_t");

        this.tree_t2_V = DateRangePrefixTree.INSTANCE;
        this.strategy_t2_V = new NumberRangePrefixTreeStrategy(tree_t2_V, name + ".t2_v");
        this.tree_t2_T = DateRangePrefixTree.INSTANCE;
        this.strategy_t2_T = new NumberRangePrefixTreeStrategy(tree_t2_T, name + ".t2_t");

        this.tree_t3_V = DateRangePrefixTree.INSTANCE;
        this.strategy_t3_V = new NumberRangePrefixTreeStrategy(tree_t3_V, name + ".t3_v");
        this.tree_t3_T = DateRangePrefixTree.INSTANCE;
        this.strategy_t3_T = new NumberRangePrefixTreeStrategy(tree_t3_T, name + ".t3_t");

        this.tree_t4_V = DateRangePrefixTree.INSTANCE;
        this.strategy_t4_V = new NumberRangePrefixTreeStrategy(tree_t4_V, name + ".t4_v");
        this.tree_t4_T = DateRangePrefixTree.INSTANCE;
        this.strategy_t4_T = new NumberRangePrefixTreeStrategy(tree_t4_T, name + ".t4_t");

        concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(BitemporalMapper.this.pattern);
            }
        };

        this.nowBitemporalDateTime = nowValue == null ?
                                     new BitemporalDateTime(Long.MAX_VALUE) :
                                     this.parseBiTemporalDate(nowValue);

    }

    public String getPattern() {
        return pattern;
    }

    public String getVtFrom() {
        return vtFrom;
    }

    public String getVtTo() {
        return vtTo;
    }

    public String getTtFrom() {
        return ttFrom;
    }

    public String getTtTo() {
        return ttTo;
    }

    public BitemporalDateTime getNowValue() {
        return this.nowBitemporalDateTime;
    }

    /**
     * Returns the {@link NumberRangePrefixTreeStrategy} of the specified tree.
     *
     * @param i                    The number of the tree [0-3].
     * @param isValidOrTransaction If the tree is of valid time or transaction time.
     * @return The {@link NumberRangePrefixTreeStrategy} of the specified tree.
     */
    public NumberRangePrefixTreeStrategy getStrategy(int i, boolean isValidOrTransaction) {
        switch (i) {
            case 0:
                return isValidOrTransaction ? strategy_t1_V : strategy_t1_T;
            case 1:
                return isValidOrTransaction ? strategy_t2_V : strategy_t2_T;
            case 2:
                return isValidOrTransaction ? strategy_t3_V : strategy_t3_T;
            case 3:
                return isValidOrTransaction ? strategy_t4_V : strategy_t4_T;
            default:
                throw new IllegalArgumentException("Not valid strategy found");
        }
    }

    /**
     * Returns the {@link DateRangePrefixTree} of the specified tree.
     *
     * @param i                    The number of the tree [0-3].
     * @param isValidOrTransaction If the tree is of valid time or transaction time.
     * @return The {@link DateRangePrefixTree} of the specified tree.
     */
    public DateRangePrefixTree getTree(int i, boolean isValidOrTransaction) {
        switch (i) {
            case 0:
                return isValidOrTransaction ? tree_t1_V : tree_t1_T;
            case 1:
                return isValidOrTransaction ? tree_t2_V : tree_t2_T;
            case 2:
                return isValidOrTransaction ? tree_t3_V : tree_t3_T;
            case 3:
                return isValidOrTransaction ? tree_t4_V : tree_t4_T;
            default:
                throw new IllegalArgumentException("Not valid tree found");
        }
    }

    /**
     * Build a {@link NRShape}.
     *
     * @param tree  The {@link DateRangePrefixTree} tree.
     * @param start The {@link BitemporalDateTime} start of the range.
     * @param stop  The {@link BitemporalDateTime} stop of the range.
     * @return A built {@link NRShape}.
     */
    public NRShape makeShape(DateRangePrefixTree tree, BitemporalDateTime start, BitemporalDateTime stop) {
        UnitNRShape startShape = tree.toUnitShape(start.toDate());
        UnitNRShape stopShape = tree.toUnitShape(stop.toDate());
        return tree.toRangeShape(startShape, stopShape);
    }

    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {
        BitemporalDateTime vt_from = readBitemporalDate(columns, this.vtFrom);
        BitemporalDateTime vt_to = readBitemporalDate(columns, this.vtTo);
        BitemporalDateTime tt_from = readBitemporalDate(columns, this.ttFrom);
        BitemporalDateTime tt_to = readBitemporalDate(columns, this.ttTo);

        if (tt_to.isNow() && vt_to.isNow()) { // T1
            Shape shapeV = makeShape(tree_t1_V, vt_from, vt_from);
            for (IndexableField field : strategy_t1_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(tree_t1_T, tt_from, tt_from);
            for (IndexableField field : strategy_t1_T.createIndexableFields(shapeT)) document.add(field);

        } else if (!tt_to.isNow() && vt_to.isNow()) {// T2
            Shape shapeV = makeShape(tree_t2_V, vt_from, vt_from);
            for (IndexableField field : strategy_t2_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(tree_t2_T, tt_from, tt_to);
            for (IndexableField field : strategy_t2_T.createIndexableFields(shapeT)) document.add(field);

        } else if (tt_to.isNow()) { // T3
            Shape shapeV = makeShape(tree_t3_V, vt_from, vt_to);
            for (IndexableField field : strategy_t3_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(tree_t3_T, tt_from, tt_from);
            for (IndexableField field : strategy_t3_T.createIndexableFields(shapeT)) document.add(field);

        } else { // T4
            Shape shapeV = makeShape(tree_t4_V, vt_from, vt_to);
            for (IndexableField field : strategy_t4_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(tree_t4_T, tt_from, tt_to);
            for (IndexableField field : strategy_t4_T.createIndexableFields(shapeT)) document.add(field);
        }
    }

    /**
     * returns a {@link BitemporalDateTime} readed from columns
     *
     * @param columns   the {@link Columns} where it is the data
     * @param fieldName the filed Name to read from {@link Columns}
     * @return a {@link BitemporalDateTime} readed from columns
     */
    BitemporalDateTime readBitemporalDate(Columns columns, String fieldName) {
        Column column = columns.getColumnsByName(fieldName).getFirst();
        if (column == null) {
            throw new IllegalArgumentException(fieldName + " column required");
        }
        return this.parseBiTemporalDate(column.getComposedValue());
    }

    BitemporalDateTime checkIfNow(BitemporalDateTime in) {
        BitemporalDateTime dateTime = in;
        if (this.nowBitemporalDateTime == null) return dateTime;
        if (dateTime.compareTo(this.nowBitemporalDateTime) == 0) {
            dateTime = new BitemporalDateTime(Long.MAX_VALUE);
        } else if (dateTime.compareTo(this.nowBitemporalDateTime) > 0) {
            throw new IllegalArgumentException("BitemporalDateTime value: " +
                                               dateTime.getTime() +
                                               " exceeds Max Value: " +
                                               this.nowBitemporalDateTime);
        }
        return dateTime;

    }

    /**
     * Parses an {@link Object} into a {@link BitemporalDateTime}. It parses {@link Long} and {@link String} format
     * values based in pattern.
     *
     * @param value The object to be parsed.
     * @return a parsed {@link BitemporalDateTime} from an {@link Object}. it parses {@link Long} and {@link String}
     * format values based in pattern.
     */
    public BitemporalDateTime parseBiTemporalDate(Object value) throws IllegalArgumentException {
        if (value != null) {
            if (value instanceof Number) {
                return checkIfNow(new BitemporalDateTime(((Number) value).longValue()));
            } else if (value instanceof String) {
                try {
                    return checkIfNow(new BitemporalDateTime(concurrentDateFormat.get()
                                                                                 .parse((String) value)
                                                                                 .getTime()));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Valid DateTime required but found " +
                                                       value +
                                                       " cannot be parsed by pattern " +
                                                       this.pattern);
                }
            } else if (value instanceof Date) {
                return checkIfNow(new BitemporalDateTime(((Date) value).getTime()));
            }
        }
        throw new IllegalArgumentException("Valid DateTime required, but found " + value);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new UnsupportedOperationException(String.format("Bitemporal mapper '%s' does not support sorting", name));
    }

    /** {@inheritDoc} */
    @Override
    public void validate(CFMetaData metaData) {
        validate(metaData, vtFrom);
        validate(metaData, vtTo);
        validate(metaData, ttFrom);
        validate(metaData, ttTo);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("vtFrom", vtFrom)
                      .add("vtTo", vtTo)
                      .add("ttFrom", ttFrom)
                      .add("ttTo", ttTo)
                      .add("pattern", pattern)
                      .add("nowValue", this.nowBitemporalDateTime)
                      .toString();
    }

    public static class BitemporalDateTime implements Comparable<BitemporalDateTime> {

        public static final BitemporalDateTime MAX = new BitemporalDateTime(Long.MAX_VALUE);
        public static final BitemporalDateTime MIN = new BitemporalDateTime(0L);

        private final Long timestamp;

        /**
         * @param date A date.
         */
        public BitemporalDateTime(Date date) {
            this.timestamp = date.getTime();
        }

        /**
         * @param timestamp A timestamp.
         */
        public BitemporalDateTime(Long timestamp) {
            if (timestamp < 0L)
                throw new IllegalArgumentException("Cannot build a BitemporalDateTime with a negative unix time");
            this.timestamp = timestamp;
        }

        public boolean isNow() {
            return timestamp.equals(MAX.timestamp);
        }

        public boolean isMin() {
            return timestamp.equals(0L);
        }

        public boolean isMax() {
            return timestamp.equals(MAX.timestamp);
        }

        public Date toDate() {
            return new Date(timestamp);
        }

        public long getTime() {
            return timestamp;
        }

        @Override
        public int compareTo(BitemporalDateTime other) {
            return timestamp.compareTo(other.timestamp);
        }

        public static BitemporalDateTime max(BitemporalDateTime bt1, BitemporalDateTime bt2) {
            int result = bt1.compareTo(bt2);
            if (result <= 0) {
                return bt2;
            } else {
                return bt1;
            }
        }

        public String toString() {
            return timestamp.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BitemporalDateTime that = (BitemporalDateTime) o;
            return timestamp.equals(that.timestamp);
        }

        @Override
        public int hashCode() {
            return timestamp.hashCode();
        }
    }
}

