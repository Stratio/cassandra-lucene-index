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

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;

import java.nio.ByteBuffer;

/**
 * Class for several row full key mappings between Cassandra and Lucene. The full key includes both the partitioning and
 * the clustering keys.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKeyMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_full_key"; //  The Lucene field name

    /** The type of the full row key, which is composed by the partition and clustering key types. */
    public CompositeType type;

    /**
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param partitionKeyMapper  A {@link PartitionKeyMapper}.
     * @param clusteringKeyMapper A {@link ClusteringKeyMapper}.
     */
    private FullKeyMapper(PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper) {
        AbstractType<?> partitionKeyType = partitionKeyMapper.getType();
        AbstractType<?> clusteringKeyType = clusteringKeyMapper.getType().asAbstractType();
        type = CompositeType.getInstance(partitionKeyType, clusteringKeyType);
    }

    /**
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param partitionKeyMapper  A {@link PartitionKeyMapper}.
     * @param clusteringKeyMapper A {@link ClusteringKeyMapper}.
     * @return A new {@link FullKeyMapper} using the specified column family metadata.
     */
    public static FullKeyMapper instance(PartitionKeyMapper partitionKeyMapper,
                                         ClusteringKeyMapper clusteringKeyMapper) {
        return new FullKeyMapper(partitionKeyMapper, clusteringKeyMapper);
    }

    /**
     * Returns the {@link ByteBuffer} representation of the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param cellName     A clustering key.
     * @return The {@link ByteBuffer} representation of the full row key formed by the specified key pair.
     */
    public ByteBuffer byteBuffer(DecoratedKey partitionKey, CellName cellName) {
        return type.builder().add(partitionKey.getKey()).add(cellName.toByteBuffer()).build();
    }

    /**
     * Adds to the specified Lucene {@link Document} the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param document      A Lucene {@link Document}.
     * @param partitionKey  A partition key.
     * @param clusteringKey A clustering key.
     */
    public void addFields(Document document, DecoratedKey partitionKey, CellName clusteringKey) {
        ByteBuffer fullKey = byteBuffer(partitionKey, clusteringKey);
        String string = ByteBufferUtils.toString(fullKey);
        Field field = new StringField(FIELD_NAME, string, Store.NO);
        document.add(field);
    }

    /**
     * Returns the Lucene {@link Term} representing the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey  A partition key.
     * @param clusteringKey A clustering key.
     * @return The Lucene {@link Term} representing the full row key formed by the specified key pair.
     */
    public Term term(DecoratedKey partitionKey, CellName clusteringKey) {
        ByteBuffer fullKey = byteBuffer(partitionKey, clusteringKey);
        return new Term(FIELD_NAME, ByteBufferUtils.toString(fullKey));
    }

}
