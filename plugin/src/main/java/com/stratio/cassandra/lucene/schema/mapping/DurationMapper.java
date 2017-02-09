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

    private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(86400000000000L);
    static final BigInteger DEFAULT_NANOS_PER_MONTH = BigInteger.valueOf(2629800000000000L); // Average

    final BigInteger nanosPerMonth;

    private final BigInteger minNanos;
    private final int numDigits;

    /**
     * Builds a new {@link DurationMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param nanosPerMonth the number on nanoseconds in a month
     */
    public DurationMapper(String field, String column, Boolean validated, Long nanosPerMonth) {
        super(field, column, validated, SUPPORTED_TYPES);
        this.nanosPerMonth = nanosPerMonth == null ? DEFAULT_NANOS_PER_MONTH : BigInteger.valueOf(nanosPerMonth);
        minNanos = nanos(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
        numDigits = serialize(minNanos).length();
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
        BigInteger nanos = nanos(duration);
        BigInteger complement = nanos.subtract(minNanos);
        return StringUtils.leftPad(serialize(complement), numDigits, '0');
    }

    private static String serialize(BigInteger number) {
        return number.toString(Character.MAX_RADIX);
    }

    BigInteger nanos(String string) {
        return nanos(Duration.from(string));
    }

    private BigInteger nanos(Duration duration) {
        return nanos(duration.getMonths(), duration.getDays(), duration.getNanoseconds());
    }

    private BigInteger nanos(int months, int days, long nanos) {
        BigInteger result = nanos == 0 ? BigInteger.ZERO : BigInteger.valueOf(nanos);
        if (days != 0) {
            result = result.add(BigInteger.valueOf(days).multiply(NANOS_PER_DAY));
        }
        if (months != 0) {
            result = result.add(BigInteger.valueOf(months).multiply(nanosPerMonth));
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("nanosPerMonth", nanosPerMonth).toString();
    }
}
