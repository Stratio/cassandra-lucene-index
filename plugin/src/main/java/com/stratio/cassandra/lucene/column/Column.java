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

package com.stratio.cassandra.lucene.column;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A cell of a CQL3 logic {@link Column}, which in most cases is different from a storage engine column.
 *
 * @param <T> The type of the column value.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class Column<T> {

    /** The map name components separator. */
    public static final String MAP_SEPARATOR = "$";

    /** The UDT name components separator. */
    public static final String UDT_SEPARATOR = ".";

    public static final String UDT_PATTERN = Pattern.quote(UDT_SEPARATOR);
    public static final String MAP_PATTERN = Pattern.quote(MAP_SEPARATOR);

    private static final Pattern NAME_PATTERN = Pattern.compile("[^(\\$|\\.)]*[\\.[^(\\$|\\.)]]*[\\$[^(\\$|\\.)]]*");

    /** The full qualified name, with UDT and map qualifiers. */
    private final String cellName;

    private final List<String> udtNames;
    private final List<String> mapNames;

    /** The column's value as {@link ByteBuffer}. */
    private final T composedValue;

    /** The column's value as {@link ByteBuffer}. */
    private final ByteBuffer decomposedValue;

    /** The column's Cassandra type. */
    private final AbstractType<T> type;

    /**
     * Builds a new {@link Column} with the specified name, name suffix, value, and type.
     *
     * @param cellName The name of the base cell.
     * @param udtNames The child UDT fields.
     * @param mapNames The child map keys.
     * @param decomposedValue The decomposed value of the column to be created.
     * @param composedValue The composed value of the column to be created.
     * @param type The type/marshaller of the column to be created.
     */
    Column(String cellName,
           List<String> udtNames,
           List<String> mapNames,
           ByteBuffer decomposedValue,
           T composedValue,
           AbstractType<T> type) {
        this.cellName = cellName;
        this.udtNames = udtNames;
        this.mapNames = mapNames;
        this.composedValue = composedValue;
        this.decomposedValue = decomposedValue;
        this.type = type;
    }

    public static ColumnBuilder builder(String cellName) {
        return new ColumnBuilder(cellName);
    }

    public static boolean isTuple(String name) {
        return name.contains(UDT_SEPARATOR);
    }

    public static void check(String name) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IndexException("Name %s doesn't satisfy the mandatory pattern %s", name, NAME_PATTERN.pattern());
        }
    }

    public static String getMapperName(String field) {
        return field.split(MAP_PATTERN)[0];
    }

    public String getMapperName() {
        return cellName + getUDTSuffix();
    }

    public static String getCellName(String field) {
        return field.split(UDT_PATTERN)[0].split(MAP_PATTERN)[0];
    }

    /**
     * Returns the column name.
     *
     * @return the column name.
     */
    public String getCellName() {
        return cellName;
    }

    /**
     * Returns the full name, which is formed by the column name and the suffix.
     *
     * @return The full name, which is formed by the column name and the suffix.
     */
    public String getFullName() {
        return cellName + getUDTSuffix() + getMapSuffix();
    }

    private String getUDTSuffix() {
        String result = "";
        for (String udtName : udtNames) {
            result += UDT_SEPARATOR + udtName;
        }
        return result;
    }

    private String getMapSuffix() {
        String result = "";
        for (String mapName : mapNames) {
            result += MAP_SEPARATOR + mapName;
        }
        return result;
    }

    /**
     * Returns the field column name appending the suffix.
     *
     * @param field A base field name.
     * @return The full column name appending the suffix.
     */
    public String getFieldName(String field) {
        return field + getMapSuffix();
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

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("fullName", getFullName())
                          .add("buildWithComposed", getComposedValue())
                          .add("type", type.getClass().getSimpleName())
                          .toString();
    }
}
