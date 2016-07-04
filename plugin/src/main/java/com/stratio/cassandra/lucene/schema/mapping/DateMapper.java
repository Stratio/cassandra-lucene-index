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

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.util.DateParser;
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;

import java.util.Date;
import java.util.Optional;

/**
 * A {@link Mapper} to map a date field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateMapper extends SingleColumnMapper.SingleFieldMapper<Long> {

    /** The date format for parsing columns. */
    public final DateParser parser;

    /**
     * Builds a new {@link DateMapper} using the specified pattern.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param pattern the date pattern
     */
    public DateMapper(String field, String column, Boolean validated, String pattern) {
        super(field,
              column,
              true,
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
        this.parser = new DateParser(pattern);
    }

    /** {@inheritDoc} */
    @Override
    protected Long doBase(String name, Object value) {
        Date date = parser.parse(value);
        return date == null ? null : date.getTime();
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> indexedField(String name, Long value) {
        return Optional.of(new LongField(name, value, STORE));
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> sortedField(String name, Long value) {
        return Optional.of(new SortedNumericDocValuesField(name, value));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortedNumericSortField(name, Type.LONG, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("pattern", parser).toString();
    }
}
