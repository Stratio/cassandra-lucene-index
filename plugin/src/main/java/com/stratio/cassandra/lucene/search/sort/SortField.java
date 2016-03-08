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

package com.stratio.cassandra.lucene.search.sort;

import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.Schema;

import java.util.Comparator;

/**
 * A sorting for a field of a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SortField {

    /** The default reverse option. */
    public static final boolean DEFAULT_REVERSE = false;

    /** {@code true} if natural order should be reversed. */
    public final boolean reverse;

    /**
     * Returns a new {@link SortField}.
     *
     * @param reverse {@code true} if natural order should be reversed.
     */
    public SortField(Boolean reverse) {

        this.reverse = reverse == null ? DEFAULT_REVERSE : reverse;
    }

    /**
     * Returns if natural order should be reversed.
     *
     * @return {@code true} if natural order should be reversed, {@code false} otherwise.
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Returns the Lucene's {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema the {@link Schema} to be used
     * @return the Lucene's sort field
     */
    public abstract org.apache.lucene.search.SortField sortField(Schema schema);

    /**
     * Returns a Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     *
     * @param schema the used {@link Schema}
     * @return the columns comparator
     */
    public abstract Comparator<Columns> comparator(Schema schema);

    @SuppressWarnings("unchecked")
    protected int compare(Comparable column1, Comparable column2) {
        if (column1 == null) {
            return column2 == null ? 0 : 1;
        } else if (column2 == null) {
            return -1;
        } else if (reverse) {
            return column2.compareTo(column1);
        } else {
            return column1.compareTo(column2);
        }
    }

    /** {@inheritDoc} */
    @Override
    public abstract String toString();

    /** {@inheritDoc} */
    @Override
    public abstract boolean equals(Object o);

    /** {@inheritDoc} */
    @Override
    public abstract int hashCode();
}