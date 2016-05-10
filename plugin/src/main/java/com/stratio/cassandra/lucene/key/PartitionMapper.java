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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Class for several partition key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class PartitionMapper {

    /**
     * The Lucene field name.
     */
    public static final String FIELD_NAME = "_partition_key";

    /**
     * The Lucene field type.
     */
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }

    // The active active partition key
    private final IPartitioner partitioner;

    // The table metadata
    private final CFMetaData metadata;

    // The mapping schema
    private final Schema schema;

    // The partition key type.
    private final AbstractType<?> type;

    /**
     * Returns a new {@code PartitionMapper} according to the specified column family meta data.
     *
     * @param metadata The column family metadata.
     * @param schema A {@link Schema}.
     */
    public PartitionMapper(CFMetaData metadata, Schema schema) {
        partitioner = DatabaseDescriptor.getPartitioner();
        this.metadata = metadata;
        this.schema = schema;
        this.type = metadata.getKeyValidator();
    }

    public AbstractType<?> getType() {
        return type;
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the specified raw partition key.
     *
     * @param document The document in which the fields are going to be added.
     * @param partitionKey The raw partition key to be converted.
     */
    public void addFields(Document document, DecoratedKey partitionKey) {
        ByteBuffer bb = partitionKey.getKey();
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Term}.
     *
     * @param partitionKey The raw partition key to be converted.
     * @return The specified raw partition key as a Lucene {@link Term}.
     */
    public Term term(DecoratedKey partitionKey) {
        ByteBuffer bb = partitionKey.getKey();
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        return new Term(FIELD_NAME, bytesRef);
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Query}.
     *
     * @param partitionKey The raw partition key to be converted.
     * @return The specified raw partition key as a Lucene {@link Query}.
     */
    public Query query(DecoratedKey partitionKey) {
        return new TermQuery(term(partitionKey));
    }

    /**
     * Returns the {@link DecoratedKey} contained in the specified Lucene {@link Document}.
     *
     * @param document the {@link Document} containing the partition key to be get.
     * @return The {@link DecoratedKey} contained in the specified Lucene {@link Document}.
     */
    public DecoratedKey partitionKey(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return partitionKey(bb);
    }

    /**
     * Returns the specified raw partition key as a a {@link DecoratedKey}.
     *
     * @param partitionKey The raw partition key to be converted.
     * @return The specified raw partition key as a a {@link DecoratedKey}.
     */
    public DecoratedKey partitionKey(ByteBuffer partitionKey) {
        return partitioner.decorateKey(partitionKey);
    }

    /**
     * Returns the columns contained in the partition key of the specified row. Note that not all the contained columns
     * are returned, but only those of the partition key.
     *
     * @param partitionKey A partition key.
     * @return The columns contained in the partition key of the specified row.
     */
    public Columns columns(DecoratedKey partitionKey) {
        Columns columns = new Columns();
        AbstractType<?> rawKeyType = metadata.getKeyValidator();
        List<ColumnDefinition> columnDefinitions = metadata.partitionKeyColumns();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            String name = columnDefinition.name.toString();
            if (schema.maps(name)) {
                ByteBuffer[] components = ByteBufferUtils.split(partitionKey.getKey(), rawKeyType);
                int position = columnDefinition.position();
                ByteBuffer value = components[position];
                AbstractType<?> valueType = rawKeyType.getComponents().get(position);
                columns.add(Column.builder(name).decomposedValue(value, valueType));
            }
        }
        return columns;
    }

    /**
     * Returns a Lucene {@link SortField} for sorting documents/rows according to the partition key.
     *
     * @return a sort field for sorting by partition key
     */
    public SortField sortField() {
        return new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        ByteBuffer bb1 = ByteBufferUtils.byteBuffer(val1);
                        ByteBuffer bb2 = ByteBufferUtils.byteBuffer(val2);
                        return ByteBufferUtil.compareUnsigned(bb1, bb2);
                    }
                };
            }
        });
    }

}
