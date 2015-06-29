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

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
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
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A {@link Mapper} to map BiTemporal DateRanges
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalMapper extends Mapper {
    public static class BiTemporalDateTime implements Comparable {

        public static BiTemporalDateTime MAX = new BiTemporalDateTime(Long.MAX_VALUE);
        public static BiTemporalDateTime MIN = new BiTemporalDateTime(0L);
        private final Long timestamp;

        /**
         *
         * @param date
         */
        public BiTemporalDateTime(Date date) {
            this.timestamp = date.getTime();
        }

        /**
         *
         * @param timestamp
         */
        public BiTemporalDateTime(Long timestamp) {
            if (timestamp<0L) throw new IllegalArgumentException("Cannot build a BiTemporalDateTime with a negative unix time");
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
        public int compareTo(Object o) {
            BiTemporalDateTime other = (BiTemporalDateTime) o;
            return timestamp.compareTo(other.timestamp);
        }

        public static BiTemporalDateTime max(BiTemporalDateTime bt1, BiTemporalDateTime bt2) {
            int result = bt1.compareTo(bt2);
            if (result <= 0) {
                return bt2;
            } else {
                return bt1;
            }
        }

        public String toString() {
            //Date date= new Date(this.timestamp);
            //return new SimpleDateFormat(DEFAULT_PATTERN).format(date);

            return timestamp.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BiTemporalDateTime that = (BiTemporalDateTime) o;
            return timestamp.equals(that.timestamp);
        }

        @Override
        public int hashCode() {
            return timestamp.hashCode();
        }
    }
    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";


    public String getPattern() {
        return pattern;
    }

    public String getVt_from() {
        return vt_from;
    }

    public String getVt_to() {
        return vt_to;
    }

    public String getTt_from() {
        return tt_from;
    }

    public String getTt_to() {
        return tt_to;
    }

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;
    /** Filed names for the four fields.   */
    private final String vt_from;
    private final String vt_to;
    private final String tt_from;
    private final String tt_to;

    // tt_to=now vt_to=now 2 daterangePrefixTree
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
     * Builds a new {@link BiTemporalMapper}.
     * @param name the name of the mapper.
     * @param vt_from The column name containing the Start Valid Time.
     * @param vt_to The column name containing the End Valid Time.
     * @param tt_from The column name containing the Start Transaction Time.
     * @param tt_to The column name containing the End Transaction Time.
     * @param pattern The {@link SimpleDateFormat} pattern to be used.
     */
    public BiTemporalMapper(String name, String vt_from, String vt_to, String tt_from, String tt_to, String pattern) {

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

        if (StringUtils.isBlank(vt_from)) {
            throw new IllegalArgumentException("vt_from column name is required");
        }

        if (StringUtils.isBlank(vt_to)) {
            throw new IllegalArgumentException("vt_to column name is required");
        }

        if (StringUtils.isBlank(tt_from)) {
            throw new IllegalArgumentException("tt_from column name is required");
        }

        if (StringUtils.isBlank(tt_to)) {
            throw new IllegalArgumentException("tt_to column name is required");
        }

        this.pattern = (pattern == null) ? DEFAULT_PATTERN : pattern;
        this.vt_from = vt_from;
        this.vt_to = vt_to;
        this.tt_from = tt_from;
        this.tt_to = tt_to;

        // Validate pattern
        new SimpleDateFormat(this.pattern);

        // tt_to=now vt_to=now 2 DateRangePrefixTree

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
                return new SimpleDateFormat(BiTemporalMapper.this.pattern);
            }
        };
    }

    /**
     * returns the {@link NumberRangePrefixTreeStrategy} of the specified tree.
     * @param i is the number of the tree [0-3].
     * @param isValidOrTransaction indicates if the tree is of valid time or transaction time.
     * @return the {@link NumberRangePrefixTreeStrategy} of the specified tree.
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
     * returns the {@link DateRangePrefixTree} of the specified tree.
     * @param i is the number of the tree [0-3].
     * @param isValidOrTransaction indicates if the tree is of valid time or transaction time.
     * @return the {@link DateRangePrefixTree} of the specified tree.
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
     * Build a {@link org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape}
     * @param tree the {@link DateRangePrefixTree} tree
     * @param start the {@link BiTemporalDateTime} start of the range
     * @param stop the {@link BiTemporalDateTime} stop of the range
     * @return a built {@link org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape}
     */
    public NumberRangePrefixTree.NRShape makeShape(DateRangePrefixTree tree,
                                                   BiTemporalDateTime start,
                                                   BiTemporalDateTime stop) {
        NumberRangePrefixTree.UnitNRShape startShape = tree.toUnitShape(start.toDate());
        NumberRangePrefixTree.UnitNRShape stopShape = tree.toUnitShape(stop.toDate());
        return tree.toRangeShape(startShape, stopShape);
    }
    /** {@inheritDoc} */
    @Override
    public void addFields(Document document, Columns columns) {

        BiTemporalDateTime vt_from = readBitemporalDate(columns, this.vt_from);
        BiTemporalDateTime vt_to = readBitemporalDate(columns, this.vt_to);
        BiTemporalDateTime tt_from = readBitemporalDate(columns, this.tt_from);
        BiTemporalDateTime tt_to = readBitemporalDate(columns, this.tt_to);

        if (tt_to.isNow() && vt_to.isNow()) { // T1
            Shape shapeV = this.makeShape(tree_t1_V, vt_from, vt_from);
            for (IndexableField field : this.strategy_t1_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = this.makeShape(tree_t1_T, tt_from, tt_from);
            for (IndexableField field : this.strategy_t1_T.createIndexableFields(shapeT)) document.add(field);

        } else if (!tt_to.isNow() && vt_to.isNow()) {// T2
            Shape shapeV = this.makeShape(tree_t2_V, vt_from, vt_from);
            for (IndexableField field : this.strategy_t2_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = this.makeShape(tree_t2_T, tt_from, tt_to);
            for (IndexableField field : this.strategy_t2_T.createIndexableFields(shapeT)) document.add(field);

        } else if (tt_to.isNow() && !vt_to.isNow()) { // T3
            Shape shapeV = this.makeShape(tree_t3_V, vt_from, vt_to);
            for (IndexableField field : this.strategy_t3_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = this.makeShape(tree_t3_T, tt_from, tt_from);
            for (IndexableField field : this.strategy_t3_T.createIndexableFields(shapeT)) document.add(field);

        } else { // T4

            Shape shapeV = this.makeShape(tree_t4_V, vt_from, vt_to);
            for (IndexableField field : this.strategy_t4_V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = this.makeShape(tree_t4_T, tt_from, tt_to);
            for (IndexableField field : this.strategy_t4_T.createIndexableFields(shapeT)) document.add(field);
        }
    }

    /**
     * returns a {@link BiTemporalDateTime} readed from columns
     * @param columns the {@link Columns} where it is the data
     * @param fieldName the filed Name to read from {@link Columns}
     * @return a {@link BiTemporalDateTime} readed from columns
     */
    BiTemporalDateTime readBitemporalDate(Columns columns, String fieldName) {
        Column column = columns.getColumnsByName(fieldName).getFirst();
        if (column == null) {
            throw new IllegalArgumentException(fieldName + " column required");
        }
        return this.parseBiTemporalDate(column.getComposedValue());
    }

    /**
     * Parses an {@link Object} into a {@link BiTemporalDateTime}. it parses {@link Long} and {@link String} format values based in pattern.
     * @param value
     * @return a parsed {@link BiTemporalDateTime} from an {@link Object}. it parses {@link Long} and {@link String} format values based in pattern.
     */
    public BiTemporalDateTime parseBiTemporalDate(Object value) {
        if (value != null) {
            if (value instanceof Number) {
                return new BiTemporalDateTime(((Number) value).longValue());
            } else if (value instanceof String) {
                try {
                    return new BiTemporalDateTime(concurrentDateFormat.get().parse(value.toString()).getTime());
                } catch (Exception e) {
                    // Ignore to fail below
                }
            } else if (value instanceof Date) {
                return new BiTemporalDateTime(((Date) value).getTime());
            }
        }
        throw new IllegalArgumentException("Valid DateTime required, but found " + value);
    }
    /** {@inheritDoc} */
    @Override
    public SortField sortField(boolean reverse) {
        throw new UnsupportedOperationException();
    }
    /** {@inheritDoc} */
    @Override
    public void validate(CFMetaData metaData) {
        validate(metaData, vt_from);
        validate(metaData, vt_to);
        validate(metaData, tt_from);
        validate(metaData, tt_to);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("vt_from", vt_from)
                .add("vt_to", vt_to)
                .add("tt_from", tt_from)
                .add("tt_to", tt_to)
                .add("pattern", pattern)
                .toString();
    }
}
