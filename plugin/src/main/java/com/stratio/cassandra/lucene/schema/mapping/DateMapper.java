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

import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.*;
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
public class DateMapper extends SingleColumnMapper.SingleFieldMapper<Long> {

    /** The date format pattern. */
    public final String pattern;

    /** The {@link DateParser}. */
    private final DateParser dateParser;

    /**
     * Builds a new {@link DateMapper} using the specified pattern.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     * @param pattern the date format pattern to be used
     */
    public DateMapper(String field, String column, Boolean indexed, Boolean sorted, Boolean validated, String pattern) {
        super(field,
              column,
              indexed,
              sorted,
              validated,
              null,
              Long.class,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              SimpleDateType.instance,
              TimestampType.instance,
              TimeUUIDType.instance);
        this.pattern = pattern == null ? DateParser.DEFAULT_PATTERN : pattern;
        this.dateParser = new DateParser(this.pattern);
    }

    /** {@inheritDoc} */
    @Override
    protected Long doBase(String name, Object value) {
        Date opt = dateParser.parse(value);
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
    public String toString() {
        return toStringHelper(this).add("pattern", pattern).toString();
    }
}
