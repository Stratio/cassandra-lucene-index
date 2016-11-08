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

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.column.ColumnsMapper;
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
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.cassandra.db.PartitionPosition.Kind.ROW_KEY;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class ClusteringMapper {

    private static final Logger logger=LoggerFactory.getLogger(ClusteringMapper.class);
    /** The Lucene field name. */
    public static final String FIELD_NAME = "_clustering";

    /** The Lucene field type. */
    private final FieldType FIELD_TYPE = new FieldType();

    /** The indexed table metadata */
    public final CFMetaData metadata;

    /** A composite type composed by a Long representing the token followed by the types of the clustering key */
    private final CompositeType globalCType;

    private void buildFieldType() {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.setDocValuesComparator((val1, val2) -> {
            ByteBuffer bb1= ByteBufferUtils.byteBuffer(val1);
            ByteBuffer bb2= ByteBufferUtils.byteBuffer(val2);
            return this.compare(bb1,bb2);
        });
        FIELD_TYPE.freeze();
    }

    /**
     *
     * @param val1
     * @param val2
     * @return
     */
    public int compare(ByteBuffer val1, ByteBuffer val2) {
        TypeSerializer<ByteBuffer> ser= this.globalCType.getSerializer();
        logger.debug("Comparign val1: {} string(val1): {} val2: {} string(val2): {}",val1, ser.toCQLLiteral(val1),val2,ser.toCQLLiteral(val2));
        return this.globalCType.compare(val1, val2);
    }

    /**
     * Constructor specifying the partition and clustering key mappers.
     *
     * @param metadata the indexed table metadata
     */
    public ClusteringMapper(CFMetaData metadata) {
        this.metadata = metadata;
        int clusteringLength= this.metadata.comparator.subtypes().size();
        AbstractType[] typesArray = new AbstractType[clusteringLength+1];
        typesArray[0]=LongType.instance;
        for(int i=0;i<clusteringLength;i++) {
            typesArray[i+1]= this.metadata.comparator.subtype(i);
        }
        globalCType =CompositeType.getInstance(typesArray);
        this.buildFieldType();
    }

    /**
     * Returns the columns contained in the specified {@link Clustering}.
     *
     * @param clustering the clustering key
     * @return the columns
     */
    public Columns columns(Clustering clustering) {
        Columns columns = new Columns();
        for (ColumnDefinition columnDefinition : metadata.clusteringColumns()) {
            String name = columnDefinition.name.toString();
            int position = columnDefinition.position();
            ByteBuffer value = clustering.get(position);
            AbstractType<?> valueType = columnDefinition.cellValueType();
            columns = columns.add(Column.apply(name).withValue(ColumnsMapper.compose(value, valueType)));
        }
        return columns;
    }

    /**
     * Returns the {@code String} human-readable representation of the specified {@link ClusteringPrefix}.
     *
     * @param prefix the clustering prefix
     * @return a {@code String} representing {@code prefix}
     */
    public String toString(ClusteringPrefix prefix) {
        return prefix == null ? null : prefix.toString(metadata);
    }

    /**
     * Returns the Lucene {@link IndexableField} representing the primary key formed by the specified primary key.
     *
     * @param key the partition key
     * @param clustering the clustering key
     * @return a indexable field
     */
    public IndexableField indexableField(DecoratedKey key, Clustering clustering) {
        ByteBuffer bb= byteBuffer(key.getToken(),clustering.clustering());
        return new Field(FIELD_NAME,ByteBufferUtils.bytesRef(bb), FIELD_TYPE);
    }

    /**
     * Returns the clustering key represented by the specified {@link ByteBuffer}.
     *
     * @param clustering a byte buffer
     * @return a Lucene field binary value
     */
    public Clustering clustering(ByteBuffer clustering) {
        ByteBuffer[] components= globalCType.split(clustering);
        return new Clustering(Arrays.copyOfRange(components,1,components.length));
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document a {@link Document} containing the clustering key to be get
     * @return the clustering key contained in {@code document}
     */
    public Clustering clustering(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return clustering(bb);
    }

    /**
     * Returns the start {@link ClusteringPrefix} of the first partition of the specified {@link DataRange}.
     *
     * @param dataRange the data range
     * @return the optional start clustering prefix of {@code dataRange}, empty if there is no such start
     */
    public static Optional<ClusteringPrefix> startClusteringPrefix(DataRange dataRange) {
        PartitionPosition startPosition = dataRange.startKey();
        Token token = startPosition.getToken();

        ClusteringIndexFilter filter;
        if (startPosition.kind() == ROW_KEY) {
            DecoratedKey startKey = (DecoratedKey) startPosition;
            filter = dataRange.clusteringIndexFilter(startKey);
        } else {
            filter = dataRange.clusteringIndexFilter(new BufferDecoratedKey(token, EMPTY_BYTE_BUFFER));
        }

        if (filter instanceof ClusteringIndexSliceFilter) {
            ClusteringIndexSliceFilter sliceFilter = (ClusteringIndexSliceFilter) filter;
            Slices slices = sliceFilter.requestedSlices();
            return Optional.of(slices.get(0).start());
        } else if (filter instanceof ClusteringIndexNamesFilter) {
            ClusteringIndexNamesFilter namesFilter = (ClusteringIndexNamesFilter) filter;
            Clustering clustering = namesFilter.requestedRows().first();
            if (clustering != null) {
                return Optional.of(clustering);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the stop {@link ClusteringPrefix} of the last partition of the specified {@link DataRange}.
     *
     * @param dataRange the data range
     * @return the optional stop clustering prefix of {@code dataRange}, empty if there is no such start
     */
    public static Optional<ClusteringPrefix> stopClusteringPrefix(DataRange dataRange) {
        PartitionPosition stopPosition = dataRange.stopKey();
        Token token = stopPosition.getToken();

        ClusteringIndexFilter filter;
        if (stopPosition.kind() == ROW_KEY) {
            DecoratedKey stopKey = (DecoratedKey) stopPosition;
            filter = dataRange.clusteringIndexFilter(stopKey);
        } else {
            filter = dataRange.clusteringIndexFilter(new BufferDecoratedKey(token, EMPTY_BYTE_BUFFER));
        }

        if (filter instanceof ClusteringIndexSliceFilter) {
            ClusteringIndexSliceFilter sliceFilter = (ClusteringIndexSliceFilter) filter;
            Slices slices = sliceFilter.requestedSlices();
            return Optional.of(slices.get(slices.size() - 1).end());
        } else if (filter instanceof ClusteringIndexNamesFilter) {
            ClusteringIndexNamesFilter namesFilter = (ClusteringIndexNamesFilter) filter;
            Clustering clustering = namesFilter.requestedRows().last();
            if (clustering != null) {
                return Optional.of(clustering);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a Lucene {@link SortField} to sort documents by primary key according to Cassandra's natural order.
     *
     * @return the sort field
     */
    public SortField sortField() {
        return new ClusteringSort(this);
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified partition slice.
     *
     * @param position the partition position
     * @param start the start clustering prefix
     * @param stop the stop clustering prefix
     * @return the Lucene query
     */
    public Query query(PartitionPosition position, ClusteringPrefix start, ClusteringPrefix stop) {
        return new ClusteringQuery(this, position, start, stop);
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering slice.
     *
     * @param key the partition key
     * @param slice the slice
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, Slice slice) {
        return query(key, slice.start(), slice.end());
    }

    /**
     * Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering slice filter.
     *
     * @param key the partition key
     * @param sliceFilter the slice filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexSliceFilter sliceFilter) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        sliceFilter.requestedSlices().forEach(slice -> builder.add(query(key, slice), SHOULD));
        return builder.build();
    }

    /**
     * Returns a Value composed by token and clustering.
     *
     * @param token a token
     * @param clustering a clustering Prefix in Query
     * @return a CompositeType ByteBuffer composed by the token and clustering values.
     */
    ByteBuffer byteBuffer(Token token, ClusteringPrefix clustering) {
        CompositeType.Builder cBuilder= globalCType.builder();
        ByteBuffer tokenBB=LongType.instance.decompose((Long)token.getTokenValue());
        cBuilder.add(tokenBB);
        for (ByteBuffer component : clustering.getRawValues()) {
            cBuilder.add(component);
        }
        return cBuilder.build();
    }

    public String toString(ByteBuffer values) {
        ByteBuffer aux= values.duplicate();
        byte[] auxB= new byte[aux.flip().remaining()];
        aux.get(auxB);
        return toString(auxB);
    }

    public String toString(byte[] values) {
        StringBuilder sb= new StringBuilder();
        for (int i=0;i<values.length;i++) {
            sb.append(values[i]);
        }
        return sb.toString();

    }

}

