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

package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

/**
 * Class for several row full key mappings between Cassandra and Lucene. The full key includes both the partitioning and
 * the clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class FullKeyMapper {

    /** The Lucene field name. */
    private static final String FIELD_NAME = "_full_key";

    /** The type of the full row key, which is composed by the partition and clustering key types. */
    private final CompositeType type;

    /** The clustering key mapper to be used. */
    private final ClusteringKeyMapper clusteringKeyMapper;

    /**
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param partitionKeyMapper  A {@link PartitionKeyMapper}.
     * @param clusteringKeyMapper A {@link ClusteringKeyMapper}.
     */
    private FullKeyMapper(PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper) {
        this.clusteringKeyMapper = clusteringKeyMapper;
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
     * @param partitionKey  A partition key.
     * @param clusteringKey A clustering key.
     * @return The {@link ByteBuffer} representation of the full row key formed by the specified key pair.
     */
    public ByteBuffer byteBuffer(DecoratedKey partitionKey, CellName clusteringKey) {
        return type.builder().add(partitionKey.getKey()).add(clusteringKey.toByteBuffer()).build();
    }

    /**
     * Returns the {@link RowKey} represented by the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The {@link RowKey} represented by the specified {@link ByteBuffer}.
     */
    public RowKey rowKey(ByteBuffer bb) {
        ByteBuffer[] bbs = ByteBufferUtils.split(bb, type);
        DecoratedKey partitionKey = DatabaseDescriptor.getPartitioner().decorateKey(bbs[0]);
        CellName clusteringKey = clusteringKeyMapper.clusteringKey(bbs[1]);
        return new RowKey(partitionKey, clusteringKey);
    }

    /**
     * Returns a hash code to uniquely identify a CQL logical row key.
     *
     * @param partitionKey  A partition key.
     * @param clusteringKey A clustering key.
     * @return A hash code to uniquely identify a CQL logical row key.
     */
    public String hash(DecoratedKey partitionKey, CellName clusteringKey) {
        return ByteBufferUtil.bytesToHex(byteBuffer(partitionKey, clusteringKey));
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
        ByteBuffer bb = byteBuffer(partitionKey, clusteringKey);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        Field field = new StringField(FIELD_NAME, bytesRef, Store.NO);
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
        ByteBuffer bb = byteBuffer(partitionKey, clusteringKey);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        return new Term(FIELD_NAME, bytesRef);
    }

}
