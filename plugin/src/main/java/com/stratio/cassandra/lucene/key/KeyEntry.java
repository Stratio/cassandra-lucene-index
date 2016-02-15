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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

import java.nio.ByteBuffer;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class KeyEntry implements Comparable<KeyEntry>{

    private final KeyMapper mapper;
    private final ByteBuffer[] components;

    public KeyEntry(KeyMapper mapper, ByteBuffer[] components) {
        this.mapper = mapper;
        this.components = components;
    }

    public Token getToken() {
        return  Murmur3Partitioner.instance.getTokenFactory().fromByteArray(components[0]);
    }

    public ByteBuffer getKey() {
        return components[1];
    }

    public DecoratedKey getDecoratedKey() {
        return DatabaseDescriptor.getPartitioner().decorateKey(getKey());
    }

    public Clustering getClustering() {
        return new Clustering(mapper.clusteringType().split(components[2]));
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(KeyEntry other) {
        int comp = getToken().compareTo(other.getToken());
        if (comp == 0) {
            comp = getDecoratedKey().compareTo(other.getDecoratedKey());
        }
        if (comp == 0) {
            comp = mapper.clusteringComparator().compare(getClustering(), other.getClustering());
        }
        return comp;
    }
}
