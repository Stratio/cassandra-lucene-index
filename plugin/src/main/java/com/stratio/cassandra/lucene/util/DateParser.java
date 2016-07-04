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
import org.apache.cassandra.utils.UUIDGen;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

    /** The {@link SimpleDateFormat} pattern. */
    public final String pattern;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> formatter;

    /**
     * Constructor with pattern.
     *
     * @param pattern the {@link SimpleDateFormat} pattern
     */
    public DateParser(String pattern) {
        this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;
        formatter = formatter(this.pattern);
    }

    private static ThreadLocal<DateFormat> formatter(final String pattern) {
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
     * Returns the {@link Date} represented by the specified {@link Object}, or {@code null} if the specified  {@link
     * Object} is {@code null}.
     *
     * @param value the {@link Object} to be parsed
     * @param <K> the type of the value to be parsed
     * @return the parsed {@link Date}
     */
    public final <K> Date parse(K value) {

        if (value == null) {
            return null;
        }

        try {
            if (value instanceof Date) {
                Date date = (Date) value;
                if (date.getTime() == Long.MAX_VALUE || date.getTime() == Long.MIN_VALUE) {
                    return date;
                } else {
                    String string = formatter.get().format(date);
                    return formatter.get().parse(string);
                }
            } else if (value instanceof UUID) {
                long timestamp = UUIDGen.unixTimestamp((UUID) value);
                Date date = new Date(timestamp);
                return formatter.get().parse(formatter.get().format(date));
            } else if (Number.class.isAssignableFrom(value.getClass())) {
                Long number = ((Number) value).longValue();
                return formatter.get().parse(number.toString());
            } else {
                return formatter.get().parse(value.toString());
            }
        } catch (Exception e) {
            throw new IndexException(e, "Error parsing {} with value '{}' using date pattern {}",
                                     value.getClass().getSimpleName(), value, pattern);
        }
    }

    public String toString(Date date) {
        return formatter.get().format(date);
    }

    public String toString() {
        return pattern;
    }
}
