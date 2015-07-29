package com.stratio.cassandra.lucene.util;

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

    /** The {@link SimpleDateFormat} pattern. */
    private String pattern;

    /** The thread safe date format. */
    private ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Constructor with pattern
     *
     * @param pattern the {@link SimpleDateFormat} pattern to use.
     */
    public DateParser(final String pattern) {
        this.pattern = pattern;

        // Validate pattern
        new SimpleDateFormat(this.pattern);

        this.concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(pattern);
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
        throw new IllegalArgumentException(String.format("Valid date required but found '%s', " +
                                                         "it can't be parsed by pattern '%s' and is not instance " +
                                                         "of Date nor Number", value, pattern));
    }
}
