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

import com.google.common.base.Objects;
import com.google.common.collect.Ordering;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A sorting of fields for a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Sort implements Iterable<SortField> {

    /** How to sortFields each field. */
    private final List<SortField> sortFields;

    /**
     * Builds a new {@link Sort} for the specified {@link SortField}s.
     *
     * @param sortFields The specified {@link SortField}s.
     */
    public Sort(List<SortField> sortFields) {
        this.sortFields = sortFields;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<SortField> iterator() {
        return sortFields.iterator();
    }

    /**
     * Returns the {@link SortField}s to be used.
     *
     * @return The {@link SortField}s to be used.
     */
    public List<SortField> getSortFields() {
        return sortFields;
    }

    /**
     * Returns the {@link Sort} representing this {@link Sort}.
     *
     * @param schema The {@link Schema} to be used.
     * @return the Lucene {@link Sort} representing this {@link Sort}.
     */
    public List<org.apache.lucene.search.SortField> sortFields(Schema schema) {
        List<org.apache.lucene.search.SortField> fields = new ArrayList<>(sortFields.size());
        for (SortField sortField : sortFields) {
            fields.add(sortField.sortField(schema));
        }
        return fields;
    }

    /**
     * Returns the {@link Columns} {@link Comparator} specified by this {@link Sort}.
     *
     * @return The {@link Columns} {@link Comparator} specified by this {@link Sort}.
     */
    public Comparator<Columns> comparator() {
        List<Comparator<Columns>> comparators = new ArrayList<>();
        for (com.stratio.cassandra.lucene.search.sort.SortField sortField : getSortFields()) {
            Comparator<Columns> comparator = sortField.comparator();
            comparators.add(comparator);
        }
        return Ordering.compound(comparators);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("sortFields", sortFields).toString();
    }
}
