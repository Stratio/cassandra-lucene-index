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

import org.apache.cassandra.db.Clustering;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.stratio.cassandra.lucene.key.ClusteringMapper.PREFIX_BYTES;
import static org.apache.cassandra.utils.FastByteOperations.compareUnsigned;

/**
 * {@link SortField} to sort by primary key.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClusteringSort extends SortField {

    /** The Lucene sort name. */
    private static final String SORT_NAME = "<clustering>";

    /**
     * Builds a new {@link ClusteringSort} for the specified {@link ClusteringMapper}.
     *
     * @param mapper the primary key mapper to be used
     */
    ClusteringSort(ClusteringMapper mapper) {
        super(ClusteringMapper.FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef t1, BytesRef t2) {
                        int comp = compareUnsigned(t1.bytes, 0, PREFIX_BYTES, t2.bytes, 0, PREFIX_BYTES);
                        if (comp == 0) {
                            ByteBuffer bb1 = ByteBuffer.wrap(t1.bytes, PREFIX_BYTES, t1.length - PREFIX_BYTES);
                            ByteBuffer bb2 = ByteBuffer.wrap(t2.bytes, PREFIX_BYTES, t2.length - PREFIX_BYTES);
                            Clustering clustering1 = mapper.clustering(bb1);
                            Clustering clustering2 = mapper.clustering(bb2);
                            comp = mapper.comparator.compare(clustering1, clustering2);
                        }
                        return comp;
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
