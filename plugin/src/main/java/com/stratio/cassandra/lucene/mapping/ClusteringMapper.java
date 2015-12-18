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

package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

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
    private final CompositeType type;

    public ClusteringMapper(CFMetaData metadata) {
        this.metadata = metadata;
        type = CompositeType.getInstance(metadata.comparator.subtypes());
    }

    public CompositeType getType() {
        return type;
    }

    /**
     * Adds the {@link Column}s contained in the specified {@link Clustering} to the specified {@link Column}s .
     *
     * @param columns    The {@link Columns} in which the {@link Clustering} {@link Column}s are going to be added.
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
     * @param document   The document where the clustering key is going to be added.
     * @param clustering A clustering key.
     */
    public void addFields(Document document, Clustering clustering) {
        BytesRef bytesRef = bytesRef(clustering);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

}
