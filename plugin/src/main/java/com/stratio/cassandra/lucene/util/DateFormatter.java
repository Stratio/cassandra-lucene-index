package com.stratio.cassandra.lucene.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by eduardoalonso on 27/07/15.
 */
public class DateFormatter {

    private String pattern;
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
    private static ThreadLocal<DateFormat> concurrentDateFormat;

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
