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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Optional;

/**
 * A {@link Mapper} to map a float field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FloatMapper extends SingleColumnMapper.SingleFieldMapper<Float> {

    /** The default boost. */
    public static final Float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    public final Float boost;

    /**
     * Builds a new {@link FloatMapper} using the specified boost.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param boost the boost
     */
    @JsonCreator
    public FloatMapper(String field, String column, Boolean validated, Float boost) {
        super(field,
              column,
              true,
              validated,
              null,
              Float.class,
              AsciiType.instance,
              ByteType.instance,
              DecimalType.instance,
              DoubleType.instance,
              FloatType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              ShortType.instance,
              UTF8Type.instance);
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /** {@inheritDoc} */
    @Override
    protected Float doBase(String name, Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value).floatValue();
            } catch (NumberFormatException e) {
                throw new IndexException("Field '{}' with value '{}' can not be parsed as float", name, value);
            }
        }
        throw new IndexException("Field '{}' requires a float, but found '{}'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> indexedField(String name, Float value) {
        FloatField floatField = new FloatField(name, value, STORE);
        floatField.setBoost(boost);
        return Optional.of(floatField);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> sortedField(String name, Float value) {
        int sortable = NumericUtils.floatToSortableInt(value);
        return Optional.of(new SortedNumericDocValuesField(name, sortable));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortedNumericSortField(name, Type.FLOAT, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("boost", boost).toString();
    }

}
