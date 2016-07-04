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

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.column.ColumnsMapper;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Class for several partition key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class PartitionMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_partition_key";

    /** The Lucene field type. */
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }

    private final CFMetaData metadata;
    private final IPartitioner partitioner;
    private final AbstractType<?> type;

    /**
     * Constructor specifying the indexed table {@link CFMetaData}.
     *
     * @param metadata the indexed table metadata
     */
    public PartitionMapper(CFMetaData metadata) {
        this.metadata = metadata;
        partitioner = DatabaseDescriptor.getPartitioner();
        type = metadata.getKeyValidator();
    }

    /**
     * Returns the type of the partition key.
     *
     * @return the key's type
     */
    public AbstractType<?> getType() {
        return type;
    }

    /**
     * Adds to the specified {@link Column} to the {@link Column}s contained in the partition key of the specified row.
     *
     * @param columns the {@link Columns} in which the {@link Column}s are going to be added
     * @param key the partition key
     */
    public void addColumns(Columns columns, DecoratedKey key) {
        List<ColumnDefinition> columnDefinitions = metadata.partitionKeyColumns();
        ByteBuffer[] components = type instanceof CompositeType
                                  ? ((CompositeType) type).split(key.getKey())
                                  : new ByteBuffer[]{key.getKey()};
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            String name = columnDefinition.name.toString();
            ByteBuffer value = components[columnDefinition.position()];
            AbstractType<?> valueType = columnDefinition.cellValueType();
            columns.add(ColumnsMapper.column(name, value, valueType));
        }
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the specified partition key.
     *
     * @param document the document in which the fields are going to be added
     * @param partitionKey the partition key to be converted
     */
    public void addFields(Document document, DecoratedKey partitionKey) {
        ByteBuffer bb = partitionKey.getKey();
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Term}.
     *
     * @param partitionKey the raw partition key to be converted
     * @return a Lucene {@link Term}
     */
    public Term term(ByteBuffer partitionKey) {
        BytesRef bytesRef = ByteBufferUtils.bytesRef(partitionKey);
        return new Term(FIELD_NAME, bytesRef);
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Term}.
     *
     * @param partitionKey the raw partition key to be converted
     * @return a Lucene {@link Term}
     */
    public Term term(DecoratedKey partitionKey) {
        return term(partitionKey.getKey());
    }

    /**
     * Returns the {@link Term} representing the partition key of the specified {@link Document}.
     *
     * @param document the document
     * @return the partition key term
     */
    public Term term(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        return new Term(FIELD_NAME, bytesRef);
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Query}.
     *
     * @param partitionKey the raw partition key to be converted
     * @return the specified raw partition key as a Lucene {@link Query}
     */
    public Query query(DecoratedKey partitionKey) {
        return new TermQuery(term(partitionKey));
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Query}.
     *
     * @param partitionKey the raw partition key to be converted
     * @return the specified raw partition key as a Lucene {@link Query}
     */
    public Query query(ByteBuffer partitionKey) {
        return new TermQuery(term(partitionKey));
    }

    /**
     * Returns the {@link DecoratedKey} contained in the specified Lucene {@link Document}.
     *
     * @param document the {@link Document} containing the partition key to be get
     * @return the {@link DecoratedKey} contained in the specified Lucene {@link Document}
     */
    public DecoratedKey decoratedKey(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return partitioner.decorateKey(bb);
    }

    /**
     * Returns a Lucene {@link SortField} for sorting documents/rows according to the partition key.
     *
     * @return a sort field for sorting by partition key
     */
    public SortField sortField() {
        return new PartitionSort(this);
    }

}
