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

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Mapper} to map durations.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DurationMapper extends KeywordMapper {

    private static final List<Class<?>> SUPPORTED_TYPES = Arrays.asList(String.class, Duration.class);

    static final BigInteger DEFAULT_NANOS_IN_DAY = BigInteger.valueOf(86400000000000L);
    static final BigInteger DEFAULT_NANOS_IN_MONTH = BigInteger.valueOf(2592000000000000L);

    final BigInteger nanosInDay;
    final BigInteger nanosInMonth;

    private final BigInteger minNanos;
    private final int numDigits;

    /**
     * Builds a new {@link DurationMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param nanosInDay the number on nanoseconds in a day
     * @param nanosInMonth the number on nanoseconds in a month
     */
    public DurationMapper(String field, String column, Boolean validated, Long nanosInDay, Long nanosInMonth) {
        super(field, column, validated, SUPPORTED_TYPES);
        this.nanosInDay = nanosInDay == null ? DEFAULT_NANOS_IN_DAY : BigInteger.valueOf(nanosInDay);
        this.nanosInMonth = nanosInDay == null ? DEFAULT_NANOS_IN_MONTH : BigInteger.valueOf(nanosInMonth);
        minNanos = bigInteger(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
        numDigits = minNanos.toString(Character.MAX_RADIX).length();
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof Duration) {
            return serialize((Duration) value);
        } else if (value instanceof String) {
            try {
                return serialize((String) value);
            } catch (InvalidRequestException e) {
                // Nothing to do here
            }
        }
        throw new IndexException("Field '{}' requires a duration, but found '{}'", name, value);
    }

    String serialize(String string) {
        return serialize(Duration.from(string));
    }

    String serialize(Duration duration) {
        BigInteger bi = bigInteger(duration);
        BigInteger complement = bi.subtract(minNanos);
        return StringUtils.leftPad(complement.toString(Character.MAX_RADIX), numDigits, '0');
    }

    BigInteger bigInteger(String string) {
        return bigInteger(Duration.from(string));
    }

    private BigInteger bigInteger(Duration duration) {
        return bigInteger(duration.getMonths(), duration.getDays(), duration.getNanoseconds());
    }

    private BigInteger bigInteger(int months, int days, long nanos) {
        BigInteger bi = nanos == 0 ? BigInteger.ZERO : BigInteger.valueOf(nanos);
        if (days != 0) {
            bi = bi.add(BigInteger.valueOf(days).multiply(nanosInDay));
        }
        if (months != 0) {
            bi = bi.add(BigInteger.valueOf(months).multiply(nanosInMonth));
        }
        return bi;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("nanosInDay", nanosInDay).add("nanosInMonth", nanosInMonth).toString();
    }
}
