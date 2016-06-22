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
package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.utils.UUIDGen;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Unified class for parse a {@link Date}s from {@link Object}s including a {@code String} pattern.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class DateParser {

    /** The default date pattern for {@code String}s. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS Z";

    /** The pattern value for timestamps. */
    public static final String TIMESTAMP_PATTERN_FIELD = "timestamp";

    private static final Long DAYS_TO_MILLIS = 24L * 60L * 60L * 1000L;

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Constructor with pattern.
     *
     * @param pattern the {@link SimpleDateFormat} pattern to use
     */
    public DateParser(String pattern) {

        this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;

        // Validate pattern if is not "timestamp"
        if (!this.pattern.equals(TIMESTAMP_PATTERN_FIELD)) {
            new SimpleDateFormat(this.pattern);
            this.concurrentDateFormat = new ThreadLocal<DateFormat>() {
                @Override
                protected DateFormat initialValue() {
                    return new SimpleDateFormat(DateParser.this.pattern);
                }
            };
            this.concurrentDateFormat.get().setLenient(false);
        } else {
            this.concurrentDateFormat = null;
        }
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Column}, or {@code null} if the value of the
     * specified {@link Column} is {@code null}.
     *
     * @param column the column to be parsed
     * @return the parsed {@link Date}
     */
    public <K> Date parse(Column<K> column) {
        if (column.getType() instanceof SimpleDateType) {
            long timestamp = SimpleDateType.instance.toTimeInMillis(column.getDecomposedValue());
            if (concurrentDateFormat == null) {
                return new Date(timestamp);
            } else {
                int offset = concurrentDateFormat.get().getTimeZone().getOffset(timestamp);
                return new Date(timestamp - offset);
            }
        } else {
            return parse(column.getComposedValue());
        }
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Object}, or {@code null} if the specified  {@link
     * Object} is {@code null}.
     *
     * @param value the {@link Object} to be parsed
     * @return the parsed {@link Date}
     */
    public Date parse(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof Integer) {
            if ((Integer) value < 0) {
                throw new IndexException("Required positive Integer for dates but found '{}'", value);
            } else {
                return new Date(DAYS_TO_MILLIS * (Integer) value);
            }
        } else if (value instanceof Long) {
            if ((Long) value < 0L) {
                throw new IndexException("Required positive Long for dates but found '{}'", value);
            } else {
                return new Date((Long) value);
            }
        } else if (value instanceof UUID) {
            try {
                return new Date(UUIDGen.unixTimestamp((UUID) value));
            } catch (UnsupportedOperationException e) {
                throw new IndexException("Required a version 1 UUID but found '{}'", value);
            }
        } else {
            if (pattern.equals(TIMESTAMP_PATTERN_FIELD)) {
                return parseAsTimestamp(value);
            } else {
                return parseAsFormattedDate(value);
            }
        }
    }

    private Date parseAsTimestamp(Object value) {
        Long valueLong;
        if (value instanceof Number) {
            valueLong = ((Number) value).longValue();
        } else {
            try {
                valueLong = Long.parseLong((value).toString());
            } catch (NumberFormatException e) {
                valueLong = null;
            }
        }
        if (valueLong != null) {
            return new Date(valueLong);
        } else {
            throw new IndexException("Valid timestamp required but found '{}'", value);
        }
    }

    private Date parseAsFormattedDate(Object value) {
        try {
            return concurrentDateFormat.get().parse(value.toString());
        } catch (ParseException e) {
            throw new IndexException("Required date with pattern '{}' but found '{}'", pattern, value);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return pattern;
    }

    /**
     * Returns the {@link String} representation of the specified {@link Date}.
     *
     * @param date the date
     * @return the {@link String} representation of {@code Date}
     */
    public String toString(Date date) {
        if (pattern.equals(TIMESTAMP_PATTERN_FIELD)) {
            return ((Long) date.getTime()).toString();
        } else {
            return concurrentDateFormat.get().format(date);
        }
    }
}
