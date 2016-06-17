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

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;

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
     * Returns the partitioning token as sortable string.
     *
     * @return the token
     */
    ByteBuffer getCollatedToken() {
        return components[0];
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
    public Clustering getClustering() {
        return new Clustering(mapper.clusteringType().split(components[2]));
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(KeyEntry other) {
        int comp = UTF8Type.instance.compare(getCollatedToken(), other.getCollatedToken());
        if (comp == 0) {
            comp = getDecoratedKey().compareTo(other.getDecoratedKey());
        }
        if (comp == 0) {
            comp = mapper.clusteringComparator().compare(getClustering(), other.getClustering());
        }
        return comp;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("collated", ByteBufferUtils.toHex(getCollatedToken()))
                          .add("key", getDecoratedKey())
                          .add("clustering", mapper.toString(getClustering()))
                          .toString();
    }

}
