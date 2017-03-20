/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.Optional;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class KeyMapper {

    public static final Logger logger = LoggerFactory.getLogger(KeyMapper.class);

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_primary_key";

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

    /** The indexed table metadata */
    private final CFMetaData metadata;

    /** The clustering key comparator */
    private final ClusteringComparator clusteringComparator;

    /** A composite type composed by the types of the clustering key */
    private final CompositeType clusteringType;

    /** The type of the primary key, which is composed by token, partition key and clustering key types. */
    private final CompositeType type;

    /**
     * Constructor specifying the partition and clustering key mappers.
     *
     * @param metadata the indexed table metadata
     */
    public KeyMapper(CFMetaData metadata) {
        this.metadata = metadata;
        clusteringComparator = metadata.comparator;
        clusteringType = CompositeType.getInstance(clusteringComparator.subtypes());
        type = CompositeType.getInstance(LongType.instance, metadata.getKeyValidator(), clusteringType);
    }

    /**
     * Returns the clustering key comparator.
     *
     * @return the comparator
     */
    public ClusteringComparator clusteringComparator() {
        return clusteringComparator;
    }

    /**
     * The type of the primary key, which is composed by token, partition key and clustering key types.
     *
     * @return the composite type
     */
    public CompositeType clusteringType() {
        return clusteringType;
    }

    /**
     * Adds the {@link Column}s contained in the specified {@link Clustering} to the specified {@link Column}s.
     *
     * @param columns the {@link Columns} in which the {@link Clustering} {@link Column}s are going to be added
     * @param clustering the clustering key
     */
    public void addColumns(Columns columns, Clustering clustering) {
        for (ColumnDefinition columnDefinition : metadata.clusteringColumns()) {
            String name = columnDefinition.name.toString();
            int position = columnDefinition.position();
            ByteBuffer value = clustering.get(position);
            AbstractType<?> valueType = columnDefinition.cellValueType();
            columns.add(Column.builder(name).buildWithDecomposed(value, valueType));
        }
    }

    /**
     * Returns the {@link KeyEntry} represented by the specified Lucene {@link BytesRef}.
     *
     * @param bytesRef the Lucene field binary value
     * @return the represented key entry
     */
    public KeyEntry entry(BytesRef bytesRef) {
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        ByteBuffer[] components = type.split(bb);
        return new KeyEntry(this, components);
    }

    /**
     * Returns the {@code String} human-readable representation of the specified {@link ClusteringPrefix}.
     *
     * @param prefix the clustering prefix
     * @return a {@code String} representing {@code prefix}
     */
    public String toString(ClusteringPrefix prefix) {
        return prefix.toString(metadata);
    }

    /**
     * Returns the {@link ByteBuffer} representation of the primary key formed by the specified partition key and the
     * clustering key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the {@link ByteBuffer} representation of the primary key
     */
    public ByteBuffer byteBuffer(DecoratedKey key, Clustering clustering) {
        return type.builder()
                   .add(TokenMapper.byteBuffer(key.getToken()))
                   .add(key.getKey())
                   .add(byteBuffer(clustering))
                   .build();
    }

    /**
     * Returns a {@link ByteBuffer} representing the specified clustering key
     *
     * @param clustering the clustering key
     * @return the byte buffer representing {@code clustering}
     */
    public ByteBuffer byteBuffer(Clustering clustering) {
        CompositeType.Builder builder = type.builder();
        for (ByteBuffer component : clustering.getRawValues()) {
            builder.add(component);
        }
        return builder.build();
    }

    /**
     * Adds to the specified Lucene {@link Document} the primary key formed by the specified partition key and the
     * clustering key.
     *
     * @param document the Lucene {@link Document} in which the key is going to be added
     * @param key the partition key
     * @param clustering the clustering key
     */
    public void addFields(Document document, DecoratedKey key, Clustering clustering) {
        ByteBuffer bb = byteBuffer(key, clustering);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        Field field = new Field(FIELD_NAME, bytesRef, FIELD_TYPE);
        document.add(field);
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

    /**
     * Returns the {@link BytesRef} representation of the specified primary key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return the Lucene field binary value
     */
    public BytesRef bytesRef(DecoratedKey key, Clustering clustering) {
        ByteBuffer bb = byteBuffer(key, clustering);
        return ByteBufferUtils.bytesRef(bb);
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document a {@link Document} containing the clustering key to be get
     * @return the clustering key contained in {@code document}
     */
    public Clustering clustering(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        return entry(bytesRef).getClustering();
    }

    /**
     * Returns the start {@link ClusteringPrefix} of the first partition of the specified {@link DataRange}.
     *
     * @param dataRange the data range
     * @return the start clustering prefix of {@code dataRange}, or {@code null} if there is no such start
     */
    public static Optional<ClusteringPrefix> startClusteringPrefix(DataRange dataRange) {
        PartitionPosition startPosition = dataRange.startKey();
        if (startPosition instanceof DecoratedKey) {
            DecoratedKey startKey = (DecoratedKey) startPosition;
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(startKey);
            if (filter instanceof ClusteringIndexSliceFilter) {
                ClusteringIndexSliceFilter sliceFilter = (ClusteringIndexSliceFilter) filter;
                Slices slices = sliceFilter.requestedSlices();
                return Optional.of(slices.get(0).start());
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the stop {@link ClusteringPrefix} of the last partition of the specified {@link DataRange}.
     *
     * @param dataRange the data range
     * @return the stop clustering prefix of {@code dataRange}, or {@code null} if there is no such start
     */
    public static Optional<ClusteringPrefix> stopClusteringPrefix(DataRange dataRange) {
        PartitionPosition stopPosition = dataRange.stopKey();
        if (stopPosition instanceof DecoratedKey) {
            DecoratedKey stopKey = (DecoratedKey) stopPosition;
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(stopKey);
            if (filter instanceof ClusteringIndexSliceFilter) {
                ClusteringIndexSliceFilter sliceFilter = (ClusteringIndexSliceFilter) filter;
                Slices slices = sliceFilter.requestedSlices();
                return Optional.of(slices.get(slices.size() - 1).end());
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the {@link Term} representing the primary key of the specified {@link Document}.
     *
     * @param document the document
     * @return the clustering key term
     */
    public static Term term(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        return new Term(FIELD_NAME, bytesRef);
    }

    /**
     * Returns a Lucene {@link SortField} to sort documents by primary key according to Cassandra's natural order.
     *
     * @return the sort field
     */
    public SortField sortField() {
        return new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        return entry(val1).compareTo(entry(val2));
                    }
                };
            }
        });
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified partition slice.
     *
     * @param key the partition key
     * @param start the start clustering prefix
     * @param stop the stop clustering prefix
     * @param acceptLowerConflicts if rows with the same token before key should be accepted
     * @param acceptUpperConflicts if rows with the same token after key should be accepted
     * @return the Lucene query
     */
    public Query query(DecoratedKey key,
                       ClusteringPrefix start,
                       ClusteringPrefix stop,
                       boolean acceptLowerConflicts,
                       boolean acceptUpperConflicts) {
        return new KeyQuery(this, key, start, stop, acceptLowerConflicts, acceptUpperConflicts);
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering slice filter.
     *
     * @param key the partition key
     * @param sliceFilter the slice filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexSliceFilter sliceFilter) {
        Slices slices = sliceFilter.requestedSlices();
        ClusteringPrefix startBound = slices.get(0).start();
        ClusteringPrefix stopBound = slices.get(slices.size() - 1).end();
        return query(key, startBound, stopBound, false, false);
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering names filter.
     *
     * @param key the partition key
     * @param namesFilter the names filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexNamesFilter namesFilter) {
        NavigableSet<Clustering> clusterings = namesFilter.requestedRows();
        if (!clusterings.isEmpty()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Clustering clustering : clusterings) {
                builder.add(query(key, clustering), SHOULD);
            }
            return builder.build();
        }
        return null;
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering filter.
     *
     * @param key the partition's key
     * @param filter the clustering filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexFilter filter) {
        if (filter instanceof ClusteringIndexNamesFilter) {
            return query(key, (ClusteringIndexNamesFilter) filter);
        } else if (filter instanceof ClusteringIndexSliceFilter) {
            return query(key, (ClusteringIndexSliceFilter) filter);
        } else {
            throw new IndexException("Unknown filter type %s", filter);
        }
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

}
