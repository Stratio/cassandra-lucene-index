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

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Class for several primary key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class KeyMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_key";

    /** The clustering key comparator */
    public final ClusteringComparator clusteringComparator;

    /** A composite type composed by the types of the clustering key */
    public final CompositeType clusteringType;

    /** The type of the primary key, which is composed by token, partition key and clustering key types. */
    private final CompositeType type;

    /**
     * Constructor specifying the partition and clustering key mappers.
     *
     * @param metadata the indexed table metadata
     */
    public KeyMapper(CFMetaData metadata) {
        clusteringComparator = metadata.comparator;
        clusteringType = CompositeType.getInstance(clusteringComparator.subtypes());
        type = CompositeType.getInstance(metadata.getKeyValidator(), clusteringType);
    }

    private ByteBuffer byteBuffer(DecoratedKey key, Clustering clustering) {
        return type.builder().add(key.getKey()).add(byteBuffer(clustering)).build();
    }

    /**
     * Returns a {@link ByteBuffer} representing the specified clustering key
     *
     * @param clustering the clustering key
     * @return the byte buffer representing {@code clustering}
     */
    private ByteBuffer byteBuffer(Clustering clustering) {
        CompositeType.Builder builder = clusteringType.builder();
        for (ByteBuffer component : clustering.getRawValues()) {
            builder.add(component);
        }
        return builder.build();
    }

    /**
     * Returns the Lucene {@link IndexableField} representing the primary key formed by the specified primary key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return a indexable field
     */
    public IndexableField indexableField(DecoratedKey key, Clustering clustering) {
        return new StringField(FIELD_NAME, bytesRef(key, clustering), Field.Store.NO);
    }

    /**
     * Returns the Lucene {@link Term} representing the primary key formed by the specified partition key and the
     * clustering key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the Lucene {@link Term} representing the primary key
     */
    public Term term(DecoratedKey key, Clustering clustering) {
        return new Term(FIELD_NAME, bytesRef(key, clustering));
    }

    private BytesRef bytesRef(DecoratedKey key, Clustering clustering) {
        ByteBuffer bb = byteBuffer(key, clustering);
        return ByteBufferUtils.bytesRef(bb);
    }

    /**
     * Returns a Lucene {@link Query} to retrieve the row with the specified primary key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, Clustering clustering) {
        return new TermQuery(term(key, clustering));
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering names filter.
     *
     * @param key the partition key
     * @param namesFilter the names filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexNamesFilter namesFilter) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        namesFilter.requestedRows().forEach(clustering -> builder.add(query(key, clustering), SHOULD));
        return builder.build();
    }

}
