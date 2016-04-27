/**
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Class representing a Cassandra's wide table primary key. This is composed by token, partition key and clustering
 * key.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
class KeyEntry implements Comparable<KeyEntry> {

    private final KeyMapper mapper;
    private final ByteBuffer[] components;

    /**
     * Constructor using a {@link KeyMapper} and an array of binary components
     *
     * @param mapper the mapper
     * @param components the binary components
     */
    KeyEntry(KeyMapper mapper, ByteBuffer[] components) {
        this.mapper = mapper;
        this.components = Arrays.copyOf(components, components.length);
    }

    /**
     * Returns the partitioning token.
     *
     * @return the token
     */
    Token getToken() {
        return Murmur3Partitioner.instance.getTokenFactory().fromByteArray(components[0]);
    }

    /**
     * Returns the raw partition key.
     *
     * @return the partition key
     */
    public ByteBuffer getKey() {
        return components[1];
    }

    /**
     * Returns the decorated partition key.
     *
     * @return the partition key
     */
    DecoratedKey getDecoratedKey() {
        return DatabaseDescriptor.getPartitioner().decorateKey(getKey());
    }

    /**
     * Returns the clustering key.
     *
     * @return the clustering key
     */

    public Composite getComposite() {
        CBuilder builder= mapper.clusteringComparator().builder();
        for (ByteBuffer bb: mapper.clusteringType().split(components[2])) {
            builder.add(bb);
        }
        return builder.build();
    }


    /** {@inheritDoc} */
    @Override
    public int compareTo(KeyEntry other) {
        int comp = getToken().compareTo(other.getToken());
        if (comp == 0) {
            comp = getDecoratedKey().compareTo(other.getDecoratedKey());
        }
        if (comp == 0) {
            comp = mapper.clusteringComparator().compare(getComposite(), other.getComposite());
        }
        return comp;
    }
}
