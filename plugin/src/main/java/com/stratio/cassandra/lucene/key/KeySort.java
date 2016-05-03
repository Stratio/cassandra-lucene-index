/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.key;

import java.io.IOException;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * {@link SortField} to sort by primary key.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class KeySort extends SortField {

    /**
     * The Lucene sort name.
     */
    private static final String SORT_NAME = "<primary_key>";

    /**
     * Builds a new {@link KeySort} for the specified {@link KeyMapper}.
     *
     * @param mapper the primary key mapper to be used.
     */
    KeySort(final KeyMapper mapper) {
        super(KeyMapper.FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
                    throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        return mapper.entry(val1).compareTo(mapper.entry(val2));
                    }
                };
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return SORT_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SortField)) {
            return false;
        }
        final SortField other = (SortField) o;
        return toString().equals(other.toString());
    }
}
