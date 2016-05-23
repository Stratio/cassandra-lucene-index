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

import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.service.RowKey;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

/**
 * Class for several row full key mappings between Cassandra and Lucene. The full key includes both the partitioning and
 * the clustering keys.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class KeyMapper {

    /**
     * The Lucene field name.
     */
    public static final String FIELD_NAME = "_primary_key";

    /**
     * The Lucene field type.
     */
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }

    /**
     * The type of the full row key, which is composed by the partition and clustering key types.
     */
    private final CompositeType type;

    private final CFMetaData metadata;

    /**
     * The type of the clustering key, which is the type of the column names.
     */
    private final CellNameType clusteringComparator;

    private final CompositeType clusteringType;

    /**
     * Returns a new {@link KeyMapper} using the specified column family metadata.
     *
     * @param metadata the indexed table {@link CFMetaData}
     */
    public KeyMapper(CFMetaData metadata) {
        this.metadata = metadata;
        clusteringComparator = metadata.comparator;
        AbstractType<?>[] subtypes = new AbstractType[clusteringComparator.size()];
        for (int i = 0; i < subtypes.length; i++) {
            subtypes[i] = clusteringComparator.subtype(i);
        }
        clusteringType = CompositeType.getInstance(subtypes);
        type = CompositeType.getInstance(UTF8Type.instance, metadata.getKeyValidator(), clusteringType);
    }

    /**
     * The type of the primary key, which is composed by token, partition key and clustering key types.
     *
     * @return the composite type
     */
    public CompositeType clusteringType() {
        return clusteringType;
    }

    CellNameType clusteringComparator() {
        return clusteringComparator;
    }

    /**
     * Returns the {@link ByteBuffer} representation of the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param clusteringKey A clustering key.
     * @return The {@link ByteBuffer} representation of the full row key formed by the specified key pair.
     */
    public ByteBuffer byteBuffer(DecoratedKey partitionKey, Composite clusteringKey) {
        return type.builder()
                   .add(TokenMapper.toCollated(partitionKey.getToken()))
                   .add(partitionKey.getKey())
                   .add(clusteringKey.toByteBuffer())
                   .build();
    }

    BytesRef seek(DecoratedKey key) {
        ByteBuffer token = TokenMapper.toCollated(key.getToken());
        return ByteBufferUtils.bytesRef(type.builder().add(token).add(EMPTY_BYTE_BUFFER).build());
    }

    /**
     * Returns the {@link RowKey} represented by the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The {@link RowKey} represented by the specified {@link ByteBuffer}.
     */
    public RowKey rowKey(ByteBuffer bb) {
        ByteBuffer[] bbs = ByteBufferUtils.split(bb, type);
        DecoratedKey partitionKey = DatabaseDescriptor.getPartitioner().decorateKey(bbs[1]);
        CellName clusteringKey = this.clusteringKey(bbs[2]);
        return new RowKey(partitionKey, clusteringKey);
    }

    /**
     * Returns a hash code to uniquely identify a CQL logical row key.
     *
     * @param partitionKey A partition key.
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
     * @param document A Lucene {@link Document}.
     * @param partitionKey A partition key.
     * @param clusteringKey A clustering key.
     */
    public void addFields(Document document, DecoratedKey partitionKey, CellName clusteringKey) {
        ByteBuffer bb = byteBuffer(partitionKey, clusteringKey);
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

    /**
     * Returns the Lucene {@link Term} representing the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param clusteringKey A clustering key.
     * @return The Lucene {@link Term} representing the full row key formed by the specified key pair.
     */
    public Term term(DecoratedKey partitionKey, CellName clusteringKey) {
        return new Term(FIELD_NAME, bytesRef(partitionKey, clusteringKey));
    }

    /**
     * Returns the {@link BytesRef} representation of the specified primary key.
     *
     * @param key the partition key
     * @param clusteringKey the clustering key
     * @return the Lucene field binary value
     */
    BytesRef bytesRef(DecoratedKey key, Composite clusteringKey) {
        ByteBuffer bb = byteBuffer(key, clusteringKey);
        return ByteBufferUtils.bytesRef(bb);
    }

    /**
     * Returns the {@link Columns} representing the data contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return The {@link Columns} representing the data contained in the specified {@link ColumnFamily}.
     */
    public Columns columns(ColumnFamily columnFamily) {
        int numClusteringColumns = metadata.clusteringColumns().size();
        Columns columns = new Columns();
        if (numClusteringColumns > 0) {
            CellName cellName = clusteringKey(columnFamily);
            if (cellName != null) {
                for (int i = 0; i < numClusteringColumns; i++) {
                    ByteBuffer value = cellName.get(i);
                    ColumnDefinition columnDefinition = metadata.clusteringColumns().get(i);
                    String name = columnDefinition.name.toString();
                    AbstractType<?> valueType = columnDefinition.type;
                    columns.add(Column.builder(name).decomposedValue(value, valueType));
                }
            }
        }
        return columns;
    }

    /**
     * Returns the first clustering key contained in the specified {@link ColumnFamily}. Note that there could be more
     * clustering keys in the column family.
     *
     * @param columnFamily A column family.
     * @return The first clustering key contained in the specified {@link ColumnFamily}.
     */
    public CellName clusteringKey(ColumnFamily columnFamily) {
        for (Cell aColumnFamily : columnFamily) {
            CellName cellName = aColumnFamily.name();
            if (!cellName.isStatic()) {
                return clusteringKey(cellName);
            }
        }
        return null;
    }

    /**
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param cellName A {@link CellName}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    private CellName clusteringKey(CellName cellName) {
        CBuilder builder = clusteringComparator.builder();
        for (int i = 0; i < metadata.clusteringColumns().size(); i++) {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        Composite prefix = builder.build();
        return clusteringComparator.rowMarker(prefix);
    }

    /**
     * Returns the clustering key contained in the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The clustering key contained in the specified {@link ByteBuffer}.
     */
    public CellName clusteringKey(ByteBuffer bb) {
        return clusteringComparator.cellFromByteBuffer(bb);
    }

    ByteBuffer clusteringKey(Composite composite) {
        CBuilder builder = clusteringComparator.builder();
        ByteBuffer[] components = clusteringType.split(composite.toByteBuffer());
        for (int i = 0; i < metadata.clusteringColumns().size(); i++) {
            ByteBuffer component = i < components.length ? components[i] : EMPTY_BYTE_BUFFER;
            builder.add(component != null ? component : EMPTY_BYTE_BUFFER);
        }
        return builder.build().toByteBuffer();
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
                       Composite start,
                       Composite stop,
                       boolean acceptLowerConflicts,
                       boolean acceptUpperConflicts) {
        return new KeyQuery(this, key, start, stop, acceptLowerConflicts, acceptUpperConflicts);
    }

    /**
     * Returns the {@link KeyEntry} represented by the specified Lucene {@link BytesRef}.
     *
     * @param bytesRef the Lucene field binary value
     * @return the represented key entry
     */
    KeyEntry entry(BytesRef bytesRef) {
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        ByteBuffer[] components = type.split(bb);
        return new KeyEntry(this, components);
    }

    /**
     * Returns a Lucene {@link SortField} to sort documents by primary key according to Cassandra's natural order.
     *
     * @return the sort field
     */
    public SortField sortField() {
        return new KeySort(this);
    }

    /**
     * Returns the storage engine column name for the specified column identifier using the specified clustering key.
     *
     * @param cellName The clustering key.
     * @param columnDefinition The column definition.
     * @return A storage engine column name.
     */
    public CellName makeCellName(CellName cellName, ColumnDefinition columnDefinition) {
        return clusteringComparator.create(start(cellName), columnDefinition);
    }

    /**
     * Returns the first possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    private Composite start(CellName cellName) {
        CBuilder builder = clusteringComparator.builder();
        for (int i = 0; i < cellName.clusteringSize(); i++) {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        return builder.build();
    }

    /**
     * Returns the last possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    private Composite end(CellName cellName) {
        return start(cellName).withEOC(Composite.EOC.END);
    }

    /**
     * Returns the specified clustering keys as an array of {@link ColumnSlice}s. It is assumed that the clustering keys
     * are docValues.
     *
     * @param clusteringKeys A docValues list of clustering keys.
     * @return The specified clustering keys as an array of {@link ColumnSlice}s.
     */
    public ColumnSlice[] columnSlices(List<CellName> clusteringKeys) {
        List<CellName> sortedClusteringKeys = sort(clusteringKeys);
        ColumnSlice[] columnSlices = new ColumnSlice[clusteringKeys.size()];
        int i = 0;
        for (CellName clusteringKey : sortedClusteringKeys) {
            Composite start = start(clusteringKey);
            Composite end = end(clusteringKey);
            ColumnSlice columnSlice = new ColumnSlice(start, end);
            columnSlices[i++] = columnSlice;
        }
        return columnSlices;
    }

    /**
     * Returns the specified list of clustering keys docValues according to the table cell name comparator.
     *
     * @param clusteringKeys The list of clustering keys to be docValues.
     * @return The specified list of clustering keys docValues according to the table cell name comparator.
     */
    private List<CellName> sort(List<CellName> clusteringKeys) {
        List<CellName> result = new ArrayList<>(clusteringKeys);
        Collections.sort(result, new Comparator<CellName>() {
            @Override
            public int compare(CellName o1, CellName o2) {
                return clusteringComparator.compare(o1, o2);
            }
        });
        return result;
    }

    /**
     * Splits the specified {@link ColumnFamily} into CQL logic rows grouping the data by clustering key.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return A map associating clustering keys with its {@link ColumnFamily}.
     */
    public Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily) {
        Map<CellName, ColumnFamily> columnFamilies = new LinkedHashMap<>();
        ColumnFamily staticColumns = null;

        if (metadata.hasStaticColumns()) {
            staticColumns = ArrayBackedSortedColumns.factory.create(metadata);
            for (Cell cell : columnFamily) {
                if (cell.name().isStatic()) {
                    staticColumns.addColumn(cell);
                }
            }
        }
        for (Cell cell : columnFamily) {
            if (!cell.name().isStatic()) {
                CellName cellName = cell.name();
                CellName clusteringKey = clusteringKey(cellName);
                ColumnFamily rowColumnFamily = columnFamilies.get(clusteringKey);
                if (rowColumnFamily == null) {
                    rowColumnFamily = ArrayBackedSortedColumns.factory.create(metadata);
                    if (staticColumns != null) {
                        rowColumnFamily.addAll(staticColumns);
                    }
                    columnFamilies.put(clusteringKey, rowColumnFamily);
                }
                rowColumnFamily.addColumn(cell);
            }
        }
        return columnFamilies;
    }

    /**
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    public CellName clusteringKey(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        ByteBuffer[] bbs = type.split(bb);
        return clusteringComparator.cellFromByteBuffer(bbs[2]);
    }

    /**
     * Returns the first clustering key contained in the specified row.
     *
     * @param row A {@link Row}.
     * @return The first clustering key contained in the specified row.
     */
    public CellName clusteringKey(Row row) {
        return clusteringKey(row.cf);
    }

    /**
     * Returns a clustering key based {@link Row} {@link Comparator}.
     *
     * @return A clustering key based {@link Row} {@link Comparator}.
     */
    public Comparator<Row> rowComparator() {
        return new Comparator<Row>() {
            @Override
            public int compare(Row row1, Row row2) {
                CellName name1 = clusteringKey(row1);
                CellName name2 = clusteringKey(row2);
                return clusteringComparator.compare(name1, name2);
            }
        };
    }

    /**
     * Returns the {@code String} human-readable representation of the specified {@link Composite}.
     *
     * @param composite the clustering {@link Composite}
     * @return a {@code String} representing {@code prefix}
     */
    public String toString(Composite composite) {
        return ByteBufferUtils.toString(composite.toByteBuffer(), clusteringType);
    }
}
