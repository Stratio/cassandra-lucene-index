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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.NumericUtils;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link Mapper} to map a float field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FloatMapper extends SingleColumnMapper<Float> {

    /** The default boost. */
    public static final Float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    private final Float boost;

    /**
     * Builds a new {@link FloatMapper} using the specified boost.
     *
     * @param name    The name of the mapper.
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     * @param boost   The boost to be used.
     */
    @JsonCreator
    public FloatMapper(String name, Boolean indexed, Boolean sorted, Float boost) {
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
    public Float getBoost() {
        return boost;
    }

    /** {@inheritDoc} */
    @Override
    public String getAnalyzer() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Float base(String name, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value).floatValue();
            } catch (NumberFormatException e) {
                // Ignore to fail below
            }
        }
        throw new IndexException("Field '%s' requires a float, but found '%s'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, Float value) {
        FloatField field = new FloatField(name, value, STORE);
        field.setBoost(boost);
        return field;
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, Float value) {
        int sortable = NumericUtils.floatToSortableInt(value);
        return new NumericDocValuesField(name, sortable);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortField(name, Type.FLOAT, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<Float> baseClass() {
        return Float.class;
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
