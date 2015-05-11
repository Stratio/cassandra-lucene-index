/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigInteger;

/**
 * A {@link ColumnMapper} to map {@link BigInteger} values. A max number of digits must be specified.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBigInteger extends ColumnMapperKeyword {

    /** The default max number of digits. */
    public static final int DEFAULT_DIGITS = 32;

    /** The max number of digits. */
    private final int digits;

    private final BigInteger complement;
    private final int hexDigits;

    /**
     * Builds a new {@link ColumnMapperBigDecimal} using the specified max number of digits.
     *
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     * @param digits  The max number of digits. If {@code null}, the {@link #DEFAULT_DIGITS} will be used.
     */
    @JsonCreator
    public ColumnMapperBigInteger(@JsonProperty("indexed") Boolean indexed,
                                  @JsonProperty("sorted") Boolean sorted,
                                  @JsonProperty("digits") Integer digits) {
        super(indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance);

        if (digits != null && digits <= 0) {
            throw new IllegalArgumentException("Positive digits required");
        }

        this.digits = digits == null ? DEFAULT_DIGITS : digits;
        complement = BigInteger.valueOf(10).pow(this.digits).subtract(BigInteger.valueOf(1));
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
    public String base(String name, Object value) {

        // Check not null
        if (value == null) {
            return null;
        }

        // Parse big decimal
        String svalue = value.toString();
        BigInteger bi;
        try {
            bi = new BigInteger(svalue);
        } catch (NumberFormatException e) {
            String message = String.format("Field %s requires a base 10 integer, but found \"%s\"", name, svalue);
            throw new IllegalArgumentException(message);
        }

        // Check size
        if (bi.abs().toString().length() > digits) {
            throw new IllegalArgumentException("Value has more than " + digits + " digits");
        }

        // Map
        bi = bi.add(complement);
        String bis = encode(bi);
        return StringUtils.leftPad(bis, hexDigits + 1, '0');
    }

    /**
     * Returns the max number of digits.
     *
     * @return The max number of digits.
     */
    public int getDigits() {
        return digits;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("digits", digits).toString();
    }
}
