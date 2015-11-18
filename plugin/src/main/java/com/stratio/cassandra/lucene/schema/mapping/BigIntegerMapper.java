/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;

/**
 * A {@link Mapper} to map {@link BigInteger} values. A max number of digits must be specified.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapper extends KeywordMapper {

    /** The default max number of digits. */
    public static final int DEFAULT_DIGITS = 32;

    /** THe numeric base. */
    private static final int BASE = 10;

    /** The max number of digits. */
    public final int digits;

    private final BigInteger complement;
    private final int hexDigits;

    /**
     * Builds a new {@link BigDecimalMapper} using the specified max number of digits.
     *
     * @param field   The name of the field.
     * @param column  The name of the column to be mapped.
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     * @param digits  The max number of digits. If {@code null}, the {@link #DEFAULT_DIGITS} will be used.
     */
    public BigIntegerMapper(String field, String column, Boolean indexed, Boolean sorted, Integer digits) {
        super(field,
              column,
              indexed,
              sorted,
              AsciiType.instance,
              ByteType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              ShortType.instance,
              UTF8Type.instance);

        if (digits != null && digits <= 0) {
            throw new IndexException("Positive digits required");
        }

        this.digits = digits == null ? DEFAULT_DIGITS : digits;
        complement = BigInteger.valueOf(BASE).pow(this.digits).subtract(BigInteger.valueOf(1));
        BigInteger maxValue = complement.multiply(BigInteger.valueOf(2));
        hexDigits = encode(maxValue).length();
    }

    /**
     * Returns the {@code String} representation of the specified {@link BigInteger}.
     *
     * @param bi The {@link BigInteger} to be converted.
     * @return The {@code String} representation of the specified {@link BigInteger}.
     */
    private static String encode(BigInteger bi) {
        return bi.toString(Character.MAX_RADIX);
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {

        // Parse big decimal
        String svalue = value.toString();
        BigInteger bi;
        try {
            bi = new BigInteger(svalue);
        } catch (NumberFormatException e) {
            throw new IndexException("Field '%s' requires a base 10 integer, but found '%s'", name, svalue);
        }

        // Check size
        if (bi.abs().toString().length() > digits) {
            throw new IndexException("Field '%s' with value '%s' has more than %d digits", name, value, digits);
        }

        // Map
        bi = bi.add(complement);
        String bis = encode(bi);
        return StringUtils.leftPad(bis, hexDigits + 1, '0');
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("digits", digits).toString();
    }
}
