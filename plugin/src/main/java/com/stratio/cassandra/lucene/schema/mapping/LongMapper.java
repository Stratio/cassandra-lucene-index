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
import com.stratio.cassandra.lucene.schema.column.Column;
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * A {@link Mapper} to map a long field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LongMapper extends SingleColumnMapper.SingleFieldMapper<Long> {

    private static final Logger logger = LoggerFactory.getLogger(LongMapper.class);
    /** The default boost. */
    public static final Float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    public final Float boost;

    /**
     * Builds a new {@link LongMapper} using the specified boost.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param boost the boost
     */
    public LongMapper(String field, String column, Boolean validated, Float boost) {
        super(field,
              column,
              true,
              validated,
              null,
              Long.class,
              AsciiType.instance,
              ByteType.instance,
              DecimalType.instance,
              DoubleType.instance,
              FloatType.instance,
              IntegerType.instance,
              Int32Type.instance,
              LongType.instance,
              ShortType.instance,
              UTF8Type.instance,
              SimpleDateType.instance,
              TimestampType.instance);
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /** {@inheritDoc} */
    @Override
    protected Long doBase(String name, Object value) {
        logger.debug("parsing an object with type: "+value.getClass()+" and value: "+value.toString());
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Date) {
            long ret=((Date) value).getTime();
            logger.debug("returning: " + Long.toString(ret));
            return ret;
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value).longValue();
            } catch (NumberFormatException e) {
                throw new IndexException("Field '%s' with value '%s' can not be parsed as long", name, value);
            }
        }
        throw new IndexException("Field '%s' requires a long, but found '%s'", name, value);
    }
    /** {@inheritDoc} */
    @Override
    protected <K> Long doBase(Column<K> column) {
        if (column.getType() instanceof SimpleDateType) {
            return SimpleDateType.instance.toTimeInMillis(column.getDecomposedValue());
        } else {
            return doBase(column.getFieldName(field), column.getComposedValue());
        }
    }
        /** {@inheritDoc} */
    @Override
    public Field indexedField(String name, Long value) {
        LongField field = new LongField(name, value, STORE);
        field.setBoost(boost);
        return field;
    }

    /** {@inheritDoc} */
    @Override
    public Field sortedField(String name, Long value) {
        return new SortedNumericDocValuesField(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortedNumericSortField(name, Type.LONG, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("boost", boost).toString();
    }
}
