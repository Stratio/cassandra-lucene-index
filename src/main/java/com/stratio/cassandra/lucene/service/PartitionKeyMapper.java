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
package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Class for several partition key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PartitionKeyMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_partition_key";

    private final IPartitioner partitioner; // The active active partition key
    private final CFMetaData metadata; // The table metadata
    private final AbstractType<?> type; // The partition key type

    /**
     * Returns a new {@code PartitionKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family metadata.
     */
    private PartitionKeyMapper(CFMetaData metadata) {
        partitioner = DatabaseDescriptor.getPartitioner();
        this.metadata = metadata;
        this.type = metadata.getKeyValidator();
    }

    /**
     * Returns a new {@code PartitionKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family metadata.
     * @return a new {@code PartitionKeyMapper} according to the specified column family meta data.
     */
    public static PartitionKeyMapper instance(CFMetaData metadata) {
        return new PartitionKeyMapper(metadata);
    }

    public AbstractType<?> getType() {
        return type;
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the specified raw partition key.
     *
     * @param document     The document in which the fields are going to be added.
     * @param partitionKey The raw partition key to be converted.
     */
    public void addFields(Document document, DecoratedKey partitionKey) {
        String serializedKey = ByteBufferUtils.toString(partitionKey.getKey());
        Field field = new StringField(FIELD_NAME, serializedKey, Store.YES);
        document.add(field);
    }

    /**
     * Returns the specified raw partition key as a Lucene {@link Term}.
     *
     * @param partitionKey The raw partition key to be converted.
     * @return The specified raw partition key as a Lucene {@link Term}.
     */
    public Term term(DecoratedKey partitionKey) {
        String serializedKey = ByteBufferUtils.toString(partitionKey.getKey());
        return new Term(FIELD_NAME, serializedKey);
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
        String string = document.get(FIELD_NAME);
        ByteBuffer partitionKey = ByteBufferUtils.fromString(string);
        return partitionKey(partitionKey);
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
     * Returns the columns contained in the partition key of the specified {@link Row}. Note that not all the contained
     * columns are returned, but only those of the partition key.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the partition key of the specified {@link Row}.
     */
    public Columns columns(Row row) {
        DecoratedKey partitionKey = row.key;
        Columns columns = new Columns();
        AbstractType<?> rawKeyType = metadata.getKeyValidator();
        List<ColumnDefinition> columnDefinitions = metadata.partitionKeyColumns();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            String name = columnDefinition.name.toString();
            ByteBuffer[] components = ByteBufferUtils.split(partitionKey.getKey(), rawKeyType);
            int position = columnDefinition.position();
            ByteBuffer value = components[position];
            AbstractType<?> valueType = rawKeyType.getComponents().get(position);
            columns.add(Column.fromDecomposed(name, value, valueType, false));
        }
        return columns;
    }

    public String toString(ByteBuffer key) {
        return ByteBufferUtils.toString(key, type);
    }

    public String toString(DecoratedKey decoratedKey) {
        return decoratedKey.getToken() + " - " + ByteBufferUtils.toString(decoratedKey.getKey(), type);
    }

}
