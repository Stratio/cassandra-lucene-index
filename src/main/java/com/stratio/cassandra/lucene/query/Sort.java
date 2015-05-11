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
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Iterator;
import java.util.List;

/**
 * A sorting of fields for a search.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Sort implements Iterable<SortField> {

    /** How to sortFields each field. */
    private final List<SortField> sortFields;

    /**
     * Builds a new {@link Sort} for the specified {@link SortField}s.
     *
     * @param sortFields The specified {@link SortField}s.
     */
    @JsonCreator
    public Sort(@JsonProperty("fields") List<SortField> sortFields) {
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
    public org.apache.lucene.search.Sort sort(Schema schema) {
        org.apache.lucene.search.SortField[] sortFields = new org.apache.lucene.search.SortField[this.sortFields.size()];
        for (int i = 0; i < this.sortFields.size(); i++) {
            sortFields[i] = this.sortFields.get(i).sortField(schema);
        }
        return new org.apache.lucene.search.Sort(sortFields);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("sortFields", sortFields).toString();
    }
}
