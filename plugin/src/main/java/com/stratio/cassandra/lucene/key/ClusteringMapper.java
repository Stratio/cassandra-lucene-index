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

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class ClusteringMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_clustering_key";

    /** The Lucene field type. */
    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }

    private final CFMetaData metadata;
    private final ClusteringComparator comparator;
    private final CompositeType type;

    public ClusteringMapper(CFMetaData metadata) {
        this.metadata = metadata;
        comparator = metadata.comparator;
        List<AbstractType<?>> subtypes = comparator.subtypes();
        type = CompositeType.getInstance(subtypes);
    }

    public CompositeType getType() {
        return type;
    }

    public ClusteringComparator getComparator() {
        return comparator;
    }

    /**
     * Adds the {@link Column}s contained in the specified {@link Clustering} to the specified {@link Column}s .
     *
     * @param columns The {@link Columns} in which the {@link Clustering} {@link Column}s are going to be added.
     * @param clustering A clustering key.
     */
    public void addColumns(Columns columns, Clustering clustering) {
        for (ColumnDefinition columnDefinition : metadata.clusteringColumns()) {
            String name = columnDefinition.name.toString();
            int position = columnDefinition.position();
            ByteBuffer value = clustering.get(position);
            AbstractType<?> type = columnDefinition.cellValueType();
            columns.add(Column.builder(name).buildWithDecomposed(value, type));
        }
    }

    public ByteBuffer byteBuffer(Clustering clustering) {
        CompositeType.Builder builder = type.builder();
        for (ByteBuffer component : clustering.getRawValues()) {
            builder.add(component);
        }
        return builder.build();
    }

    public BytesRef bytesRef(Clustering clustering) {
        ByteBuffer bb = byteBuffer(clustering);
        return ByteBufferUtils.bytesRef(bb);
    }

    /**
     * Adds to the specified document the clustering key contained in the specified cell name.
     *
     * @param document The document where the clustering key is going to be added.
     * @param clustering A clustering key.
     */
    public void addFields(Document document, Clustering clustering) {
        BytesRef bytesRef = bytesRef(clustering);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

    /**
     * Returns the clustering key contained in the specified {@link ByteBuffer}.
     *
     * @param bb a {@link ByteBuffer}
     * @return the clustering key contained in the specified {@link ByteBuffer}
     */
    public Clustering clustering(ByteBuffer bb) {
        return new Clustering(type.split(bb));
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document a {@link Document}
     * @return the clustering key contained in the specified {@link Document}
     */
    public Clustering clustering(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        return clustering(bytesRef);
    }

    /**
     * Returns the {@link Clustering} contained in the specified Lucene field value.
     *
     * @param bytesRef the {@link BytesRef} containing the raw clustering key to be get
     * @return the {@link Clustering} contained in the specified Lucene field value
     */
    public Clustering clustering(BytesRef bytesRef) {
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return clustering(bb);
    }

    /**
     * Returns a Lucene {@link SortField} for sorting documents/rows according to the clustering key.
     *
     * @return a Lucene {@link SortField} for sorting documents/rows according to the clustering key
     */
    public SortField sortField() {
        return new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        Clustering bb1 = clustering(val1);
                        Clustering bb2 = clustering(val2);
                        return metadata.comparator.compare(bb1, bb2);
                    }
                };
            }
        });
    }

    public Filter filter(ClusteringPrefix start, ClusteringPrefix stop) {
        Query query  = new ClusteringQuery(start, stop, this);
        return new QueryWrapperFilter(query);
    }

    /**
     * Returns the {@code String} human-readable representation of the specified clustering prefix.
     *
     * @param prefix the clustering prefix
     * @return the {@code String} human-readable representation of the specified clustering prefix
     */
    public String toString(ClusteringPrefix prefix) {
        return prefix.toString(metadata);
    }

}
