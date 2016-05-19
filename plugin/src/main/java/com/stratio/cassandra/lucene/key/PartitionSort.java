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
package com.stratio.cassandra.lucene.key;

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link SortField} to sort by partition key.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class PartitionSort extends SortField {

    /** The Lucene sort name. */
    private static final String SORT_NAME = "<partition_key>";

    /**
     * Builds a new {@link PartitionSort} for the specified {@link PartitionMapper}.
     *
     * @param mapper the partition key mapper to be used
     */
    PartitionSort(PartitionMapper mapper) {
        super(PartitionMapper.FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        ByteBuffer bb1 = ByteBufferUtils.byteBuffer(val1);
                        ByteBuffer bb2 = ByteBufferUtils.byteBuffer(val2);
                        return mapper.getType().compare(bb1, bb2);
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return SORT_NAME;
    }

    /** {@inheritDoc} */
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
