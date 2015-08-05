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

package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unified class for parse a {@link Date}s from {@link Object}s including a {@code String} pattern.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class DateParser {

    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS z";

    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Constructor with pattern
     *
     * @param pattern the {@link SimpleDateFormat} pattern to use.
     */
    public DateParser(String pattern) {

        this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;

        // Validate pattern
        new SimpleDateFormat(this.pattern);

        this.concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(DateParser.this.pattern);
            }
        };
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Object}, or {@code null} if the specified  {@link
     * Object} is {@code null}.
     *
     * @param value The {@link Object} to pe parsed.
     * @return The {@link Date} represented by the specified {@link Object}.
     */
    public Date parse(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }
        try {
            return concurrentDateFormat.get().parse(value.toString());
        } catch (ParseException e) {
            // Ignore
        }
        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }
        throw new IndexException("Valid date required but found '%s', it can't be parsed by pattern '%s' " +
                                 "and is not instance of Date nor Number", value, pattern);
    }

    @Override
    public String toString() {
        return pattern;
    }
}
