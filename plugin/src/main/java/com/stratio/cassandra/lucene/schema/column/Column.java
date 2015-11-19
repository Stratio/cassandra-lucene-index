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

package com.stratio.cassandra.lucene.schema.column;

import com.google.common.base.Objects;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

/**
 * A cell of a CQL3 logic {@link Column}, which in most cases is different from a storage engine column.
 *
 * @param <T> The type of the column value.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class Column<T> implements Comparable<Column<?>> {

    /** The column's lucene field name. Used in Conditions */
    private final String fieldName;

    /** The column's mapperName. */
    private final String mapperName;

    /** The default mapKeys separator. */
    public static final String mapSeparator = "$";

    /** The default mapKeys separator. */
    public static final String udtSeparator = ".";

    /** The column's value as {@link ByteBuffer}. */
    private final T composedValue;

    /** The column's value as {@link ByteBuffer}. */
    private final ByteBuffer decomposedValue;

    /** The column's Cassandra type. */
    private final AbstractType<T> type;

    private final boolean isMultiCell;

    /**
     * Builds a new {@link Column} with the specified name, name suffix, value, and type.
     *
     * @param fieldName       The field name of the column to be created.
     * @param decomposedValue The decomposed value of the column to be created.
     * @param composedValue   The composed value of the column to be created.
     * @param type            The type/marshaller of the column to be created.
     * @param isMultiCell     If the column is a multiCell column (not frozen Collections).
     */
    private Column(String fieldName,
                   ByteBuffer decomposedValue,
                   T composedValue,
                   AbstractType<T> type,
                   boolean isMultiCell) {
        this.fieldName = fieldName;
        this.mapperName = getMapperNameByFullName(fieldName);
        this.composedValue = composedValue;
        this.decomposedValue = decomposedValue;
        this.type = type;
        this.isMultiCell = isMultiCell;
    }

    public static String getMapperNameByFullName(String input) {
        if (input.contains(".")) {
            String[] components = input.split(Pattern.quote("."));
            String out = "";
            for (int i = 0; i < components.length; i++) {

                if (components[i].contains(Column.mapSeparator)) {
                    String[] auxComponents = components[i].split(Pattern.quote(Column.mapSeparator));
                    out += auxComponents[0];
                } else {
                    out += components[i];
                }
                if (i < components.length - 1) {
                    out += ".";
                }
            }
            return out;
        } else {
            if (input.contains(Column.mapSeparator)) {
                String[] auxComponents = input.split(Pattern.quote(Column.mapSeparator));
                return auxComponents[0];
            }
            return input;
        }
    }

    public static String joinMapItemName(String input, String mapItemName) {
        return input == null ? mapItemName : input + mapSeparator + mapItemName;

    }

    public static String joinUDTItemName(String input, String udtChildItemName) {
        return input == null ? udtChildItemName : input + udtSeparator + udtChildItemName;
    }

    /**
     * Returns the column name.
     *
     * @return the column name.
     */
    public String getMapperName() {
        return mapperName;
    }

    /**
     * Returns the full name, which is formed by the column name and suffix.
     *
     * @return The full name, which is formed by the column name and suffix.
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Returns the full column name appending the suffix.
     *
     * @param name A column name.
     * @return The full column name appending the suffix.
     */
    public String getFieldName(String name) {
        return name + this.fieldName;
    }

    /**
     * Returns the {@link ByteBuffer} serialized value.
     *
     * @return the {@link ByteBuffer} serialized value.
     */
    public ByteBuffer getDecomposedValue() {
        return decomposedValue;
    }

    /**
     * Returns the Java column value.
     *
     * @return The Java column value.
     */
    public T getComposedValue() {
        return composedValue;
    }

    /**
     * Returns the Cassandra column type.
     *
     * @return The Cassandra column type.
     */
    public AbstractType<T> getType() {
        return type;
    }

    /**
     * Returns if this Column is a multiCell column (not frozen Collections).
     *
     * @return if this Column is a multiCell column (not frozen Collections).
     */
    public boolean isMultiCell() {
        return isMultiCell;
    }

    /**
     * Returns the {@link Column} defined by the specified name, raw value and type.
     *
     * @param name            The column name.
     * @param decomposedValue The column raw value.
     * @param type            The column type/marshaller.
     * @param isMultiCell     If the {@link Column} is a multiCell column (not frozen Collections).
     * @param <T>             The base type.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromDecomposed(String name,
                                               ByteBuffer decomposedValue,
                                               AbstractType<T> type,
                                               boolean isMultiCell) {
        T composedValue = type.compose(decomposedValue);
        return new Column<>(name, decomposedValue, composedValue, type, isMultiCell);
    }

    /**
     * Returns the {@link Column} defined by the specified name, value and type.
     *
     * @param name          The column name.
     * @param composedValue The column composed value.
     * @param type          The column type/marshaller.
     * @param isMultiCell   If the {@link Column} is a multiCell column (not frozen Collections).
     * @param <T>           The base type.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromComposed(String name, T composedValue, AbstractType<T> type, boolean isMultiCell) {
        ByteBuffer decomposedValue = type.decompose(composedValue);
        return new Column<>(name, decomposedValue, composedValue, type, isMultiCell);
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(Column<?> column2) {
        if (column2 == null) {
            return 1;
        }
        ByteBuffer value1 = decomposedValue;
        ByteBuffer value2 = column2.getDecomposedValue();
        return type.compare(value1, value2);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("fieldName", getFieldName())
                      .add("composedValue", getComposedValue())
                      .add("type", type.getClass().getSimpleName())
                      .toString();
    }
}
