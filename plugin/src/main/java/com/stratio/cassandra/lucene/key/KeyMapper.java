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

package com.stratio.cassandra.lucene.key;

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class KeyMapper {

    /** The Lucene field name. */
    private static final String FIELD_NAME = "_full_key";

    /** The type of the full row key, which is composed by the partition and clustering key types. */
    private final CompositeType type;

    /** The clustering key mapper to be used. */
    private final ClusteringMapper clusteringMapper;

    public KeyMapper(PartitionMapper partitionMapper, ClusteringMapper clusteringMapper) {
        this.clusteringMapper = clusteringMapper;
        AbstractType<?> partitionKeyType = partitionMapper.getType();
        CompositeType clusteringKeyType = clusteringMapper.getType();
        type = CompositeType.getInstance(partitionKeyType, clusteringKeyType);
    }

    /**
     * Returns the {@link ByteBuffer} representation of the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param key A partition key.
     * @param clustering A clustering key.
     * @return The {@link ByteBuffer} representation of the full row key formed by the specified key pair.
     */
    public ByteBuffer byteBuffer(DecoratedKey key, Clustering clustering) {
        return type.builder().add(key.getKey()).add(clusteringMapper.byteBuffer(clustering)).build();
    }

    /**
     * Adds to the specified Lucene {@link Document} the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param document A Lucene {@link Document}.
     * @param key A partition key.
     * @param clustering A clustering key.
     */
    public void addFields(Document document, DecoratedKey key, Clustering clustering) {
        ByteBuffer bb = byteBuffer(key, clustering);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        Field field = new StringField(FIELD_NAME, bytesRef, Field.Store.NO);
        document.add(field);
    }

    /**
     * Returns the Lucene {@link Term} representing the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param key A partition key.
     * @param clustering A clustering key.
     * @return The Lucene {@link Term} representing the full row key formed by the specified key pair.
     */
    public Term term(DecoratedKey key, Clustering clustering) {
        ByteBuffer bb = byteBuffer(key, clustering);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        return new Term(FIELD_NAME, bytesRef);
    }

}
