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
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;

import java.util.Optional;

/**
 * A {@link Mapper} to map a double field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapper extends SingleColumnMapper.SingleFieldMapper<Double> {

    /** The default boost. */
    public static final float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    public final Float boost;

    /**
     * Builds a new {@link DoubleMapper} using the specified boost.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param boost the boost
     */
    public DoubleMapper(String field, String column, Boolean validated, Float boost) {
        super(field,
              column,
              true,
              validated,
              null,
              Double.class,
              AsciiType.instance,
              ByteType.instance,
              DecimalType.instance,
              DoubleType.instance,
              FloatType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              UTF8Type.instance,
              ShortType.instance,
              UTF8Type.instance);
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /** {@inheritDoc} */
    @Override
    protected Double doBase(String name, Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value);
            } catch (NumberFormatException e) {
                throw new IndexException("Field '{}' with value '{}' can not be parsed as double", name, value);
            }
        }
        throw new IndexException("Field '{}' requires a double, but found '{}'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> indexedField(String name, Double value) {
        DoubleField doubleField = new DoubleField(name, value, STORE);
        doubleField.setBoost(boost);
        return Optional.of(doubleField);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> sortedField(String name, Double value) {
        long sortable = NumericUtils.doubleToSortableLong(value);
        return Optional.of(new SortedNumericDocValuesField(name, sortable));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortedNumericSortField(name, Type.DOUBLE, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("boost", boost).toString();
    }
}
