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

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A sorted list of CQL3 logic {@link Column}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Columns implements Iterable<Column<?>> {

    /** The wrapped columns. */
    private final List<Column<?>> columns;

    /** Returns an empty {@link Column} list. */
    public Columns() {
        this.columns = new LinkedList<>();
    }

    /**
     * Returns an empty {@link Column} list.
     *
     * @param initialCapacity the initial capacity of the list
     */
    public Columns(int initialCapacity) {
        this.columns = new ArrayList<>(initialCapacity);
    }

    /**
     * Returns a new {@link Columns} composed by the specified {@link Column}s.
     *
     * @param columns A list of {@link Column}s.
     */
    public Columns(Column<?>... columns) {
        this.columns = Arrays.asList(columns);
    }

    /**
     * Adds the specified {@link Column} to the existing ones.
     *
     * @param column the {@link Column} to be added.
     * @return this with the specified {@link Column}
     */
    public Columns add(Column<?> column) {
        columns.add(column);
        return this;
    }

    /**
     * Returns a {@link ColumnAdder} for adding a {@link Column} with the specified cell name.
     *
     * @param cellName the cell name of the {@link Column} to be added
     * @return a column adder
     */
    public ColumnAdder adder(String cellName) {
        return new ColumnAdder(this, Column.builder(cellName));
    }

    /**
     * Returns a {@link ColumnAdder} for adding a {@link Column} with the specified cell name and deletion time.
     *
     * @param cellName the cell name of the {@link Column} to be added
     * @param deletionTime the deletion time  of the {@link Column} to be added, in seconds
     * @return a column adder
     */
    public ColumnAdder adder(String cellName, int deletionTime) {
        return new ColumnAdder(this, Column.builder(cellName, deletionTime));
    }

    /**
     * Adds a new {@link Column} with the specified name, composed value and type.
     *
     * @param name the column name
     * @param value the composed value
     * @param <T> the base class
     * @return this with the specified {@link Column}
     */
    public <T> Columns add(String name, T value) {
        return add(Column.build(name, value));
    }

    /**
     * Returns an {@link Iterator} over the {@link Column}s in insert order.
     *
     * @return an iterator in insert order
     */
    public Iterator<Column<?>> iterator() {
        return columns.iterator();
    }

    /**
     * Returns a {@link Stream} over the {@link Column}s in insert order.
     *
     * @return a stream in insert order
     */
    public Stream<Column<?>> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Returns the number of {@link Column}s in this list. If this list contains more than <tt>Integer.MAX_VALUE</tt>
     * elements, returns <tt>Integer.MAX_VALUE</tt>.
     *
     * @return The number of {@link Column}s in this list
     */
    public int size() {
        return columns.size();
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    /**
     * Returns the {@link Column} identified by the specified full name, or {@code null} if not found.
     *
     * @param name The full name of the {@link Column} to be returned.
     * @return The {@link Column} identified by the specified full name, or {@code null} if not found.
     */
    public Columns getByFullName(String name) {
        Column.check(name);
        Columns result = new Columns();
        columns.forEach(column -> {
            if (column.getFullName().equals(name)) {
                result.add(column);
            }
        });
        return result;
    }

    /**
     * Returns the {@link Column} identified by the specified CQL cell name, or {@code null} if not found.
     *
     * @param name The CQL cell name of the{@link Column} to be returned.
     * @return The {@link Column} identified by the specified CQL cell name, or {@code null} if not found.
     */
    public Columns getByCellName(String name) {
        Column.check(name);
        String cellName = Column.getCellName(name);
        Columns result = new Columns();
        columns.forEach(column -> {
            if (column.getCellName().equals(cellName)) {
                result.add(column);
            }
        });
        return result;
    }

    /**
     * Returns the {@link Column} identified by the specified mapper name, or {@code null} if not found.
     *
     * @param name The mapper name of the {@link Column} to be returned.
     * @return The {@link Column} identified by the specified mapper name, or {@code null} if not found.
     */
    public Columns getByMapperName(String name) {
        Column.check(name);
        String mapperName = Column.getMapperName(name);
        Columns result = new Columns();
        columns.forEach(column -> {
            if (column.getMapperName().equals(mapperName)) {
                result.add(column);
            }
        });
        return result;
    }

    public Column<?> getFirst() {
        return columns.isEmpty() ? null : columns.get(0);
    }

    /**
     * Returns the non expired columns from this list using now value.
     *
     * @param nowInSec the max allowed time in seconds
     * @return a copy of this without the columns expired before {@code now}
     */
    public Columns cleanDeleted(int nowInSec) {
        Columns clean = new Columns();
        columns.forEach(column -> {
            if (column.isDeleted(nowInSec)) {
                clean.add(column);
            }
        });
        return clean;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (Column<?> column : columns) {
            helper.add(column.getFullName(), column.getValue());
        }
        return helper.toString();
    }
}
