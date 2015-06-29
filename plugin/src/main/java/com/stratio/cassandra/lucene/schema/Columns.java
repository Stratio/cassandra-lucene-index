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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A sorted list of CQL3 logic {@link Column}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Columns implements Iterable<Column> {

    /** The wrapped columns. */
    private final List<Column> columns;

    /** Constructs an empty {@link Column} list. */
    public Columns() {
        this.columns = new LinkedList<>();
    }

    public Columns(Column... columns) {
        this.columns = Arrays.asList(columns);
    }

    /**
     * Adds the specified {@link Column} to the existing ones.
     *
     * @param column the {@link Column} to be added.
     * @return this
     */
    public Columns add(Column column) {
        columns.add(column);
        return this;
    }

    /**
     * Adds the specified {@link Column}s to the existing ones.
     *
     * @param columns The {@link Column}s to be added.
     * @return this {@link Columns} with the specified {@link Column}s.
     */
    public Columns add(Columns columns) {
        for (Column column : columns) {
            this.columns.add(column);
        }
        return this;
    }

    /**
     * Returns an iterator over the {@link Column}s in insert order.
     *
     * @return An iterator over the {@link Column}s in insert order.
     */
    public Iterator<Column> iterator() {
        return columns.iterator();
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
     * Returns the {@link Column} identified by the specified name, or {@code null} if not found.
     *
     * @param name The name of the {@link Column} to be returned.
     * @return The {@link Column} identified by the specified name, or {@code null} if not found.
     */
    public Columns getColumnsByFullName(String name) {
        Columns result = new Columns();
        for (Column column : columns) {
            if (column.getFullName().equals(name)) {
                result.add(column);
            }
        }
        return result;
    }

    /**
     * Returns the {@link Column} identified by the specified name, or {@code null} if not found.
     *
     * @param name The name of the {@link Column} to be returned.
     * @return The {@link Column} identified by the specified name, or {@code null} if not found.
     */
    public Columns getColumnsByName(String name) {
        Columns result = new Columns();
        for (Column column : columns) {
            if (column.getName().equals(name)) {
                result.add(column);
            }
        }
        return result;
    }

    public Column getFirst() {
        return columns.isEmpty() ? null : columns.get(0);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("columns", columns).toString();
    }
}
