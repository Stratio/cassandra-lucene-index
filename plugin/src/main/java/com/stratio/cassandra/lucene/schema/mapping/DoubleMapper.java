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
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.NumericUtils;

/**
 * A {@link Mapper} to map a double field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapper extends SingleColumnMapper<Double> {

    /** The default boost. */
    public static final float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    private final Float boost;

    /**
     * Builds a new {@link DoubleMapper} using the specified boost.
     *
     * @param name    The name of the mapper.
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     * @param boost   The boost to be used.
     */
    public DoubleMapper(String name, Boolean indexed, Boolean sorted, Float boost) {
        super(name,
              indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              Int32Type.instance,
              LongType.instance,
              IntegerType.instance,
              FloatType.instance,
              DoubleType.instance,
              DecimalType.instance);
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /**
     * Returns the boost to be used.
     *
     * @return The boost to be used.
     */
    public float getBoost() {
        return boost;
    }

    /** {@inheritDoc} */
    @Override
    public Double base(String name, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value);
            } catch (NumberFormatException e) {
                // Ignore to fail below
            }
        }
        throw new IndexException("Field '%s' requires a double, but found '%s'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, Double value) {
        DoubleField field = new DoubleField(name, value, STORE);
        field.setBoost(boost);
        return field;
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, Double value) {
        long sortable = NumericUtils.doubleToSortableLong(value);
        return new NumericDocValuesField(name, sortable);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortField(name, Type.DOUBLE, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<Double> baseClass() {
        return Double.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("indexed", indexed)
                      .add("sorted", sorted)
                      .add("boost", boost)
                      .toString();
    }
}
