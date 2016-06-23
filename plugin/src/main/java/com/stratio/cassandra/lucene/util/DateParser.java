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
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.UUIDGen;

import javax.validation.constraints.NotNull;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Unified class for parsing {@link Date}s from {@link Object}s and {@link Column}s.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class DateParser {

    /** The default date pattern for parsing {@code String}s and truncations. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS Z";

    /** The pattern value for timestamps. */
    public static final String TIMESTAMP_PATTERN = "timestamp";

    /** The {@link SimpleDateFormat} pattern for columns. */
    public final String columnPattern;

    /** The {@link SimpleDateFormat} pattern for fields. */
    public final String fieldPattern;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> columnFormatter;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> fieldFormatter;

    /**
     * Constructor with pattern.
     *
     * @param pattern the {@link SimpleDateFormat} pattern to use
     */
    public DateParser(String pattern) {
        this(pattern, pattern);
    }

    /**
     * Constructor with pattern.
     *
     * @param columnPattern the {@link SimpleDateFormat} pattern for columns
     * @param fieldPattern the {@link SimpleDateFormat} pattern for fields
     */
    public DateParser(String columnPattern, String fieldPattern) {
        this.columnPattern = columnPattern == null ? DEFAULT_PATTERN : columnPattern;
        this.fieldPattern = fieldPattern == null ? DEFAULT_PATTERN : fieldPattern;
        columnFormatter = formatter(this.columnPattern);
        fieldFormatter = formatter(this.fieldPattern);
    }

    public DateParser(String defaultPattern, String columnPattern, String fieldPattern) {
        this(columnPattern == null ? defaultPattern : columnPattern,
             fieldPattern == null ? defaultPattern : fieldPattern);
    }

    private static ThreadLocal<DateFormat> formatter(final String pattern) {
        if (pattern.equals(TIMESTAMP_PATTERN)) {
            return null;
        }
        new SimpleDateFormat(pattern);
        ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(pattern);
            }
        };
        formatter.get().setLenient(false);
        return formatter;
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Column}, or {@code null} if the value of the
     * specified {@link Column} is {@code null}.
     *
     * @param column the column to be parsed
     * @return the parsed {@link Date}
     */
    public final <K> Date parse(Column<K> column) {
        if (column == null || column.getDecomposedValue() == null) {
            return null;
        }

        AbstractType<?> type = column.getType();
        K value = column.getComposedValue();
        try {
            Date date;
            if (type instanceof SimpleDateType) {
                long timestamp = SimpleDateType.instance.toTimeInMillis(column.getDecomposedValue());
                if (columnFormatter != null) {
                    timestamp -= columnFormatter.get().getTimeZone().getOffset(timestamp);
                }
                date = new Date(timestamp);
            } else if (type instanceof TimestampType) {
                date = (Date) value;
            } else if (type instanceof UUIDType || type instanceof TimeUUIDType) {
                date = date((UUID) value);
            } else if (columnFormatter != null) {
                date = columnFormatter.get().parse(value.toString());
            } else if (Number.class.isAssignableFrom(value.getClass())) {
                date = new Date(((Number) value).longValue());
            } else {
                date = new Date(Long.parseLong((value).toString()));
            }
            return formatField(date);
        } catch (Exception e) {
            throw new IndexException("Required date with pattern '{}' but found '{}' with value '{}'",
                                     columnPattern, value.getClass().getSimpleName(), value);
        }
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Object}, or {@code null} if the specified  {@link
     * Object} is {@code null}.
     *
     * @param value the {@link Object} to be parsed
     * @return the parsed {@link Date}
     */
    public final <K> Date parse(K value) {

        if (value == null) {
            return null;
        }

        try {
            if (value instanceof Date) {
                return formatField((Date) value);
            } else if (value instanceof UUID) {
                return formatField(date((UUID) value));
            } else if (fieldFormatter != null) {
                return fieldFormatter.get().parse(value.toString());
            } else if (Number.class.isAssignableFrom(value.getClass())) {
                return formatField(((Number) value).longValue());
            } else {
                return new Date(Long.parseLong((value).toString()));
            }
        } catch (Exception e) {
            throw new IndexException(e, "Required date with pattern '{}' but found '{}' with value '{}'",
                                     fieldPattern, value.getClass().getSimpleName(), value);
        }
    }

    private Date formatField(Long timestamp) throws ParseException {
        return formatField(new Date(timestamp));
    }

    private Date formatField(Date date) throws ParseException {
        Long timestamp = date.getTime();
        if (fieldFormatter == null || timestamp == Long.MIN_VALUE || timestamp == Long.MAX_VALUE) {
            return date;
        }
        return fieldFormatter.get().parse(fieldFormatter.get().format(date));
    }

    private static Date date(@NotNull UUID uuid) {
        try {
            return new Date(UUIDGen.unixTimestamp(uuid));
        } catch (UnsupportedOperationException e) {
            throw new IndexException("Required a version 1 UUID but found '{}'", uuid);
        }
    }

    /**
     * Returns the {@link String} representation of the specified {@link Date}.
     *
     * @param date the date
     * @return the {@link String} representation of {@code Date}
     */
    public String toString(Date date) {
        if (date == null) {
            return null;
        } else if (fieldFormatter == null) {
            return ((Long) date.getTime()).toString();
        } else {
            return fieldFormatter.get().format(date);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return columnPattern.equals(fieldPattern)
               ? columnPattern
               : String.format("{column=%s,field=%s}", columnPattern, fieldPattern);
    }
}
