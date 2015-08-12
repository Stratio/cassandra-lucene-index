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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;

import java.util.Date;

/**
 * A {@link Mapper} to map a date field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateMapper extends SingleColumnMapper<Long> {

    /** The date format pattern. */
    private final String pattern;

    /** The {@link DateParser}. */
    private final DateParser dateParser;

    /**
     * Builds a new {@link DateMapper} using the specified pattern.
     *
     * @param name    The name of the mapper.
     * @param column  The name of the column to be mapped.
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     * @param pattern The date format pattern to be used.
     */
    public DateMapper(String name, String column, Boolean indexed, Boolean sorted, String pattern) {
        super(name,
              column,
              indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              DecimalType.instance,
              TimestampType.instance);
        this.pattern = pattern == null ? DateParser.DEFAULT_PATTERN : pattern;
        this.dateParser = new DateParser(this.pattern);
    }

    /**
     * Returns the {@link SimpleDateFormat} pattern to be used.
     *
     * @return The {@link SimpleDateFormat} pattern to be used.
     */
    public String getPattern() {
        return pattern;
    }

    /** {@inheritDoc} */
    @Override
    public String getAnalyzer() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Long base(String name, Object value) {
        Date opt = this.dateParser.parse(value);
        if (opt == null) {
            return null;
        } else {
            return opt.getTime();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, Long value) {
        return new LongField(name, value, STORE);
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, Long value) {
        return new NumericDocValuesField(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortField(name, Type.LONG, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<Long> baseClass() {
        return Long.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("pattern", pattern).toString();
    }
}
