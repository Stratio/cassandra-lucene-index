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
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * A {@link Mapper} to map bitemporal DateRanges.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapper extends Mapper {

    /** The {@link DateParser} pattern. */
    private final String pattern;

    /** The {@link DateParser} */
    private final DateParser dateParser;

    /** Field names for the four fields. */
    private final String vtFrom;
    private final String vtTo;
    private final String ttFrom;
    private final String ttTo;

    private Long nowBitemporalDateTimeMillis;

    // ttTo=now vtTo=now 2 DateRangePrefixTree
    private NumberRangePrefixTreeStrategy strategyT1V;
    private DateRangePrefixTree treeT1V;
    private NumberRangePrefixTreeStrategy strategyT1T;
    private DateRangePrefixTree treeT1T;

    private NumberRangePrefixTreeStrategy strategyT2V;
    private DateRangePrefixTree treeT2V;
    private NumberRangePrefixTreeStrategy strategyT2T;
    private DateRangePrefixTree treeT2T;

    private NumberRangePrefixTreeStrategy strategyT3V;
    private DateRangePrefixTree treeT3V;
    private NumberRangePrefixTreeStrategy strategyT3T;
    private DateRangePrefixTree treeT3T;

    private NumberRangePrefixTreeStrategy strategyT4V;
    private DateRangePrefixTree treeT4V;
    private NumberRangePrefixTreeStrategy strategyT4T;
    private DateRangePrefixTree treeT4T;

    /**
     * Builds a new {@link BitemporalMapper}.
     *
     * @param name    the name of the mapper.
     * @param vtFrom  The column name containing the valid time start.
     * @param vtTo    The column name containing the valid time stop.
     * @param ttFrom  The column name containing the transaction time start.
     * @param ttTo    The column name containing the transaction time stop.
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
              Arrays.<AbstractType>asList(AsciiType.instance,
                                          UTF8Type.instance,
                                          Int32Type.instance,
                                          LongType.instance,
                                          IntegerType.instance,
                                          FloatType.instance,
                                          DoubleType.instance,
                                          DecimalType.instance,
                                          TimestampType.instance),
              Arrays.asList(vtFrom, vtTo, ttFrom, ttTo));

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

        this.pattern = pattern == null ? DateParser.DEFAULT_PATTERN : pattern;
        this.dateParser = new DateParser(this.pattern);

        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;

        // Validate pattern

        // ttTo=now vtTo=now 2 DateRangePrefixTree

        treeT1V = DateRangePrefixTree.INSTANCE;
        strategyT1V = new NumberRangePrefixTreeStrategy(treeT1V, name + ".t1_v");
        treeT1T = DateRangePrefixTree.INSTANCE;
        strategyT1T = new NumberRangePrefixTreeStrategy(treeT1T, name + ".t1_t");

        treeT2V = DateRangePrefixTree.INSTANCE;
        strategyT2V = new NumberRangePrefixTreeStrategy(treeT2V, name + ".t2_v");
        treeT2T = DateRangePrefixTree.INSTANCE;
        strategyT2T = new NumberRangePrefixTreeStrategy(treeT2T, name + ".t2_t");

        treeT3V = DateRangePrefixTree.INSTANCE;
        strategyT3V = new NumberRangePrefixTreeStrategy(treeT3V, name + ".t3_v");
        treeT3T = DateRangePrefixTree.INSTANCE;
        strategyT3T = new NumberRangePrefixTreeStrategy(treeT3T, name + ".t3_t");

        treeT4V = DateRangePrefixTree.INSTANCE;
        strategyT4V = new NumberRangePrefixTreeStrategy(treeT4V, name + ".t4_v");
        treeT4T = DateRangePrefixTree.INSTANCE;
        strategyT4T = new NumberRangePrefixTreeStrategy(treeT4T, name + ".t4_t");

        nowBitemporalDateTimeMillis = (nowValue == null) ? Long.MAX_VALUE : dateParser.parse(nowValue).getTime();

    }

    public String getPattern() {
        return pattern;
    }

    /**
     * Returns the column name containing the valid time start.
     *
     * @return The column name containing the valid time start.
     */
    public String getVtFrom() {
        return vtFrom;
    }

    /**
     * Returns the column name containing the valid time stop.
     *
     * @return The column name containing the valid time stop.
     */
    public String getVtTo() {
        return vtTo;
    }

    /**
     * Returns the column name containing the transaction time start.
     *
     * @return The column name containing the transaction time start.
     */
    public String getTtFrom() {
        return ttFrom;
    }

    /**
     * Returns the column name containing the transaction time stop.
     *
     * @return The column name containing the transaction time stop.
     */
    public String getTtTo() {
        return ttTo;
    }

    /**
     * Returns the now value as UNIX timestamp.
     *
     * @return The now value as UNIX timestamp.
     */
    public Long getNowValue() {
        return nowBitemporalDateTimeMillis;
    }

    /** {@inheritDoc} */
    @Override
    public String getAnalyzer() {
        return null;
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
                return isValidOrTransaction ? strategyT1V : strategyT1T;
            case 1:
                return isValidOrTransaction ? strategyT2V : strategyT2T;
            case 2:
                return isValidOrTransaction ? strategyT3V : strategyT3T;
            case 3:
                return isValidOrTransaction ? strategyT4V : strategyT4T;
            default:
                throw new IndexException("Not valid strategy found");
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
                return isValidOrTransaction ? treeT1V : treeT1T;
            case 1:
                return isValidOrTransaction ? treeT2V : treeT2T;
            case 2:
                return isValidOrTransaction ? treeT3V : treeT3T;
            case 3:
                return isValidOrTransaction ? treeT4V : treeT4T;
            default:
                throw new IndexException("Not valid tree found");
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

        BitemporalDateTime vtFrom = readBitemporalDate(columns, this.vtFrom);
        BitemporalDateTime vtTo = readBitemporalDate(columns, this.vtTo);
        BitemporalDateTime ttFrom = readBitemporalDate(columns, this.ttFrom);
        BitemporalDateTime ttTo = readBitemporalDate(columns, this.ttTo);

        if (vtFrom == null && vtTo == null && ttFrom == null && ttTo == null) {
            return;
        } else if (vtFrom == null) {
            throw new IndexException("vt_from column required");
        } else if (vtTo == null) {
            throw new IndexException("vt_to column required");
        } else if (ttFrom == null) {
            throw new IndexException("tt_from column required");
        } else if (ttTo == null) {
            throw new IndexException("tt_to column required");
        }

        if (ttTo.isNow() && vtTo.isNow()) { // T1
            Shape shapeV = makeShape(treeT1V, vtFrom, vtFrom);
            for (IndexableField field : strategyT1V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(treeT1T, ttFrom, ttFrom);
            for (IndexableField field : strategyT1T.createIndexableFields(shapeT)) document.add(field);

        } else if (!ttTo.isNow() && vtTo.isNow()) {// T2
            Shape shapeV = makeShape(treeT2V, vtFrom, vtFrom);
            for (IndexableField field : strategyT2V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(treeT2T, ttFrom, ttTo);
            for (IndexableField field : strategyT2T.createIndexableFields(shapeT)) document.add(field);

        } else if (ttTo.isNow()) { // T3
            Shape shapeV = makeShape(treeT3V, vtFrom, vtTo);
            for (IndexableField field : strategyT3V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(treeT3T, ttFrom, ttFrom);
            for (IndexableField field : strategyT3T.createIndexableFields(shapeT)) document.add(field);

        } else { // T4
            Shape shapeV = makeShape(treeT4V, vtFrom, vtTo);
            for (IndexableField field : strategyT4V.createIndexableFields(shapeV)) document.add(field);

            Shape shapeT = makeShape(treeT4T, ttFrom, ttTo);
            for (IndexableField field : strategyT4T.createIndexableFields(shapeT)) document.add(field);
        }
    }

    /**
     * returns a {@link BitemporalDateTime} read from columns
     *
     * @param columns   the {@link Columns} where it is the data
     * @param fieldName the filed Name to read from {@link Columns}
     * @return a {@link BitemporalDateTime} read from columns
     */
    BitemporalDateTime readBitemporalDate(Columns columns, String fieldName) {
        Column column = columns.getColumnsByName(fieldName).getFirst();
        if (column == null) {
            return null;
        }
        return parseBiTemporalDate(column.getComposedValue());
    }

    private BitemporalDateTime checkIfNow(Long in) {
        if (in > nowBitemporalDateTimeMillis) {
            throw new IndexException("BitemporalDateTime value '%s' exceeds Max Value: '%s'",
                                     in,
                                     nowBitemporalDateTimeMillis);
        } else if (in < nowBitemporalDateTimeMillis) {
            return new BitemporalDateTime(in);
        } else {
            return new BitemporalDateTime(Long.MAX_VALUE);
        }
    }

    /**
     * Parses an {@link Object} into a {@link BitemporalDateTime}. It parses {@link Long} and {@link String} format
     * values based in pattern.
     *
     * @param value The object to be parsed.
     * @return a parsed {@link BitemporalDateTime} from an {@link Object}. it parses {@link Long} and {@link String}
     * format values based in pattern.
     */
    public BitemporalDateTime parseBiTemporalDate(Object value) {
        Date opt = dateParser.parse(value);
        if (opt != null) {
            return checkIfNow(opt.getTime());
        } else {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException(String.format("Bitemporal mapper '%s' does not support sorting", name));
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
                      .add("nowValue", nowBitemporalDateTimeMillis)
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
            timestamp = date.getTime();
        }

        /**
         * @param timestamp A timestamp.
         */
        public BitemporalDateTime(Long timestamp) {
            if (timestamp < 0L) throw new IndexException("Cannot build a BitemporalDateTime with a negative unix time");
            this.timestamp = timestamp;
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
            return new Date(timestamp);
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

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return timestamp.toString();
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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

