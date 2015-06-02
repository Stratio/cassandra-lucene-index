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
package com.stratio.cassandra.lucene.schema;

import com.google.common.base.Objects;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

/**
 * A cell of a CQL3 logic {@link Column}, which in most cases is different from a storage engine column.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Column<T> {

    /** The column's name. */
    private final String name;

    /** The column's name sufix used for maps. */
    private final String nameSufix;

    /** The column's value as {@link ByteBuffer}. */
    private final T composedValue;

    /** The column's value as {@link ByteBuffer}. */
    private final ByteBuffer decomposedValue;

    /** The column's Cassandra type. */
    private final AbstractType<T> type;

    private final boolean isCollection;

    /**
     * Builds a new {@link Column} with the specified name, name sufix, value, and type.
     *
     * @param name            The name of the column to be created.
     * @param nameSufix       The name sufix of the column to be created.
     * @param decomposedValue The decomposed value of the column to be created.
     * @param composedValue   The composed value of the column to be created.
     * @param type            The type/marshaller of the column to be created.
     */
    private Column(String name,
                   String nameSufix,
                   ByteBuffer decomposedValue,
                   T composedValue,
                   AbstractType<T> type,
                   boolean isCollection) {
        this.name = name;
        this.nameSufix = nameSufix;
        this.composedValue = composedValue;
        this.decomposedValue = decomposedValue;
        this.type = type;
        this.isCollection = isCollection;
    }

    /**
     * Returns the column name.
     *
     * @return the column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the full name, which is formed by the column name and sufix.
     *
     * @return The full name, which is formed by the column name and sufix.
     */
    public String getFullName() {
        return nameSufix == null ? name : name + "." + nameSufix;
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

    public boolean isCollection() {
        return isCollection;
    }

    /**
     * Returns the {@link Column} defined by the specified name, raw value and type.
     *
     * @param name            The column name.
     * @param decomposedValue The column raw value.
     * @param type            The column type/marshaller.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromDecomposed(String name,
                                               ByteBuffer decomposedValue,
                                               AbstractType<T> type,
                                               boolean isCollection) {
        T composedValue = type.compose(decomposedValue);
        return new Column<>(name, null, decomposedValue, composedValue, type, isCollection);
    }

    /**
     * Returns the {@link Column} defined by the specified name, raw value and type.
     *
     * @param name            The column name.
     * @param nameSufix       The column name sufix.
     * @param decomposedValue The column raw value.
     * @param type            The column type/marshaller.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromDecomposed(String name,
                                               String nameSufix,
                                               ByteBuffer decomposedValue,
                                               AbstractType<T> type,
                                               boolean isCollection) {
        T composedValue = type.compose(decomposedValue);
        return new Column<>(name, nameSufix, decomposedValue, composedValue, type, isCollection);
    }

    /**
     * Returns the {@link Column} defined by the specified name, value and type.
     *
     * @param name          The column name.
     * @param composedValue The column composed value.
     * @param type          The column type/marshaller.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromComposed(String name, T composedValue, AbstractType<T> type, boolean isCollection) {
        ByteBuffer decomposedValue = type.decompose(composedValue);
        return new Column<>(name, null, decomposedValue, composedValue, type, isCollection);
    }

    /**
     * Returns the {@link Column} defined by the specified name, value and type.
     *
     * @param name          The column name.
     * @param composedValue The column composed value.
     * @param type          The column type/marshaller.
     * @return A {@link Column}.
     */
    public static <T> Column<T> fromComposed(String name, String sufix, T composedValue, AbstractType<T> type, boolean isCollection) {
        ByteBuffer decomposedValue = type.decompose(composedValue);
        return new Column<>(name, sufix, decomposedValue, composedValue, type, isCollection);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("fullName", getFullName())
                      .add("composedValue", getComposedValue())
                      .add("type", type.getClass().getSimpleName())
                      .toString();
    }
}
