package com.stratio.cassandra.lucene.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unified class for parse a Date from Object including a String pattern
 * @author Eduardo ALonso {@literal <eduardoalonso@stratio.com>}
 */
public class DateFormatter {

    /** The {@link SimpleDateFormat} pattern. */
    private String pattern;

    /** The thread safe date format. */
    private static ThreadLocal<DateFormat> concurrentDateFormat;

    /**
     * Constructor with pattern
     * @param pattern the {@link SimpleDateFormat} pattern to use.
     */
    public DateFormatter(String pattern) {
        this.pattern=pattern;
        //validate pattern
        new SimpleDateFormat(this.pattern);

        this.concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(DateFormatter.this.pattern);
            }
        };
    }

    /**
     * Returns a Date from the Object
     * @param value the Object value to pe parsed.
     * @return a Date that matches the pattern or null if the object cant be parsed
     */
    public Date fromObject(Object value) {
        if (value == null) {
            return null;
        } else {
            if (value instanceof Date) {
                return (Date) value;
            }
            try {
                return concurrentDateFormat.get().parse(value.toString());
            } catch (ParseException e) {

            }
            if (value instanceof Number) {
                return new Date(((Number) value).longValue());
            }
            throw new IllegalArgumentException("Valid DateTime required but found " +
                    value + " cannot be parsed by pattern " + this.pattern+ " and is not instance of Date, Number");

        }
    }
}
