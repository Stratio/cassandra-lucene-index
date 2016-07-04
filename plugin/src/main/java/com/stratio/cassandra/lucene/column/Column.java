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
    private final T value;

    /** The deletion time in seconds. */
    private final Integer deletionTime;

    /**
     * Builds a new {@link Column} with the specified name, name suffix, value, and type.
     *
     * @param cellName the name of the base cell
     * @param udtNames he child UDT fields
     * @param mapNames the child map keys
     * @param value the composed value of the column to be created
     * @param deletionTime the deletion time in seconds
     */
    Column(String cellName,
           List<String> udtNames,
           List<String> mapNames,
           T value,
           Integer deletionTime) {
        this.cellName = cellName;
        this.udtNames = udtNames;
        this.mapNames = mapNames;
        this.value = value;
        this.deletionTime = deletionTime;
    }

    /**
     * Builds a new {@link Column} with the specified name, composed value and type.
     *
     * @param name the column name
     * @param value the composed value
     * @param <T> the base type
     * @return a new column
     */
    public static <T> Column<T> build(String name, T value) {
        return builder(name).build(value);
    }

    /**
     * Returns a new {@link ColumnBuilder} using the specified base cell name and deletion time.
     *
     * @param cellName the base cell name
     * @param deletionTime the deletion time in seconds
     * @return the column builder
     */
    public static ColumnBuilder builder(String cellName, int deletionTime) {
        return new ColumnBuilder(cellName, deletionTime);
    }

    /**
     * Returns a new {@link ColumnBuilder} using the specified base cell name and no deletion time.
     *
     * @param cellName the base cell name
     * @return the column builder
     */
    public static ColumnBuilder builder(String cellName) {
        return builder(cellName, Integer.MAX_VALUE);
    }

    /**
     * Returns if the specified name belongs to a tuple.
     *
     * @param name the name
     * @return {@code true} if the name belongs to a tuple, {@code false} otherwise
     */
    public static boolean isTuple(String name) {
        return name.contains(UDT_SEPARATOR);
    }

    /**
     * Checks if the specified name is syntactically correct.
     *
     * @param name the name to be checked
     */
    public static void check(String name) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IndexException("Name {} doesn't satisfy the mandatory pattern {}", name, NAME_PATTERN.pattern());
        }
    }

    /**
     * Returns the mapper name component of the specified field name.
     *
     * @param field a field name
     * @return the mapper name component
     */
    public static String getMapperName(String field) {
        return field.split(MAP_PATTERN)[0];
    }

    public String getMapperName() {
        return cellName + getUDTSuffix();
    }

    /**
     * Returns the cell name of the specified field name.
     *
     * @param field the field name
     * @return the cell name
     */
    public static String getCellName(String field) {
        return field.split(UDT_PATTERN)[0].split(MAP_PATTERN)[0];
    }

    /**
     * Returns the cell name.
     *
     * @return the cell name
     */
    public String getCellName() {
        return cellName;
    }

    /**
     * Returns the full name, which is formed by the column name and the suffix.
     *
     * @return the full name
     */
    public String getFullName() {
        return cellName + getUDTSuffix() + getMapSuffix();
    }

    /**
     * Returns the UDT suffix.
     *
     * @return the UDT suffix
     */
    private String getUDTSuffix() {
        String result = "";
        for (String udtName : udtNames) {
            result += UDT_SEPARATOR + udtName;
        }
        return result;
    }

    /**
     * Returns the map suffix.
     *
     * @return the map suffix
     */
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
     * @return the field name
     */
    public String getFieldName(String field) {
        return field + getMapSuffix();
    }

    /**
     * Returns the Java column value.
     *
     * @return the composed value
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns if the column is deleted. A column is considered deleted if its value is {@code null} or if its deletion
     * time is before than the specified time.
     *
     * @param nowInSec the max allowed time in seconds
     * @return {@code true} if the column is a deletion, {@code false} otherwise
     */
    public boolean isDeleted(int nowInSec) {
        return value == null || deletionTime != null && nowInSec < deletionTime;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("cell", cellName)
                          .add("name", getFullName())
                          .add("value", getValue())
                          .add("deletionTime", deletionTime)
                          .toString();
    }
}
