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

import com.stratio.cassandra.lucene.codecs.SerializableSortField;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MultiSorter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link SortField} to sort by partition key.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PartitionSort extends SerializableSortField {

    /** The Lucene sort name. */
    private static final String SORT_NAME = "<partition>";

    private PartitionMapper mapper;

    public PartitionSort() {
        super();
    }

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
        this.mapper=mapper;
    }
    public PartitionSort read(DataInput input) throws IOException {
        String keyspace=input.readString();
        String table=input.readString();
        CFMetaData metadata= Schema.instance.getCFMetaData(keyspace,table);
        return new PartitionSort(PartitionMapper.instance(metadata));
    }
    public void write(DataOutput output) throws IOException {
        output.writeString(this.mapper.metadata.ksName);
        output.writeString(this.mapper.metadata.cfName);
    }

    @Override
    public MultiSorter.CrossReaderComparator getCrossReaderComparator(List<CodecReader> readers) throws IOException {
        List<BinaryDocValues> values = new ArrayList<>();
        for(CodecReader reader : readers)
            values.add(DocValues.getBinary(reader,this.getField()));

        return (readerIndexA, docIDA, readerIndexB, docIDB) -> {
            BytesRef valueA= values.get(readerIndexA).get(docIDA);
            BytesRef valueB = values.get(readerIndexB).get(docIDB);
            ByteBuffer bb1 = ByteBufferUtils.byteBuffer(valueA);
            ByteBuffer bb2 = ByteBufferUtils.byteBuffer(valueB);
            return mapper.getType().compare(bb1, bb2);
        };
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
