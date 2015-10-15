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

package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class must be used only with column
 * families with wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class ClusteringKeyMapper {

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

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_clustering_key";

    /** The column family meta data. */
    private final CFMetaData metadata;

    /** The mapping schema. */
    private final Schema schema;

    /** The type of the clustering key, which is the type of the column names. */
    private final CellNameType cellNameType;

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @param schema   A {@link Schema}.
     */
    private ClusteringKeyMapper(CFMetaData metadata, Schema schema) {
        this.metadata = metadata;
        this.schema = schema;
        cellNameType = metadata.comparator;
    }

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @param schema   A {@link Schema}.
     * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
     */
    public static ClusteringKeyMapper instance(CFMetaData metadata, Schema schema) {
        return new ClusteringKeyMapper(metadata, schema);
    }

    /**
     * Returns the clustering key validation type. It's always a {@link CellNameType} in CQL3 tables.
     *
     * @return The clustering key validation type.
     */
    public CellNameType getType() {
        return cellNameType;
    }

    /**
     * Adds to the specified document the clustering key contained in the specified cell name.
     *
     * @param document The document where the clustering key is going to be added.
     * @param cellName A cell name containing the clustering key to be added.
     */
    public void addFields(Document document, CellName cellName) {
        BytesRef bytesRef = bytesRef(cellName);
        document.add(new Field(FIELD_NAME, bytesRef, FIELD_TYPE));
    }

    /**
     * Returns the first clustering key contained in the specified {@link ColumnFamily}. Note that there could be more
     * clustering keys in the column family.
     *
     * @param columnFamily A column family.
     * @return The first clustering key contained in the specified {@link ColumnFamily}.
     */
    public CellName clusteringKey(ColumnFamily columnFamily) {
        Iterator<Cell> iterator = columnFamily.iterator();
        return iterator.hasNext() ? clusteringKey(iterator.next().name()) : null;
    }

    /**
     * Returns the clustering key contained in the specified {@link ByteBuffer}.
     *
     * @param bb A {@link ByteBuffer}.
     * @return The clustering key contained in the specified {@link ByteBuffer}.
     */
    public CellName clusteringKey(ByteBuffer bb) {
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the storage engine column name for the specified column identifier using the specified clustering key.
     *
     * @param cellName         The clustering key.
     * @param columnDefinition The column definition.
     * @return A storage engine column name.
     */
    public CellName makeCellName(CellName cellName, ColumnDefinition columnDefinition) {
        return cellNameType.create(start(cellName), columnDefinition);
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
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    public CellName clusteringKey(Document document) {
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the clustering key contained in the specified Lucene field value.
     *
     * @param bytesRef The {@link BytesRef} containing the raw clustering key to be get.
     * @return The clustering key contained in the specified Lucene field value.
     */
    public CellName clusteringKey(BytesRef bytesRef) {
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return clusteringKey(bb);
    }

    /**
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param cellName A {@link CellName}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    private CellName clusteringKey(CellName cellName) {
        CBuilder builder = cellNameType.builder();
        for (int i = 0; i < metadata.clusteringColumns().size(); i++) {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        Composite prefix = builder.build();
        return cellNameType.rowMarker(prefix);
    }

    BytesRef bytesRef(CellName clusteringKey) {
        ByteBuffer bb = clusteringKey.toByteBuffer();
        return ByteBufferUtils.bytesRef(bb);
    }

    /**
     * Returns the first possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    private Composite start(CellName cellName) {
        CBuilder builder = cellNameType.builder();
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
                    if (schema.maps(name)) {
                        AbstractType<?> valueType = columnDefinition.type;
                        columns.add(Column.fromDecomposed(name, value, valueType, false));
                    }
                }
            }
        }
        return columns;
    }

    /**
     * Splits the specified {@link ColumnFamily} into CQL logic rows grouping the data by clustering key.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return A map associating clustering keys with its {@link ColumnFamily}.
     */
    public Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily) {
        Map<CellName, ColumnFamily> columnFamilies = new LinkedHashMap<>();
        for (Cell cell : columnFamily) {
            CellName cellName = cell.name();
            CellName clusteringKey = clusteringKey(cellName);
            ColumnFamily rowColumnFamily = columnFamilies.get(clusteringKey);
            if (rowColumnFamily == null) {
                rowColumnFamily = ArrayBackedSortedColumns.factory.create(metadata);
                columnFamilies.put(clusteringKey, rowColumnFamily);
            }
            rowColumnFamily.addColumn(cell);

        }
        return columnFamilies;
    }

    /**
     * Returns the specified clustering keys as an array of {@link ColumnSlice}s. It is assumed that the clustering keys
     * are sorted.
     *
     * @param clusteringKeys A sorted list of clustering keys.
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
     * Returns the specified list of clustering keys sorted according to the table cell name comparator.
     *
     * @param clusteringKeys The list of clustering keys to be sorted.
     * @return The specified list of clustering keys sorted according to the table cell name comparator.
     */
    private List<CellName> sort(List<CellName> clusteringKeys) {
        List<CellName> result = new ArrayList<>(clusteringKeys);
        Collections.sort(result, new Comparator<CellName>() {
            @Override
            public int compare(CellName o1, CellName o2) {
                return cellNameType.compare(o1, o2);
            }
        });
        return result;
    }

    /**
     * Returns a Lucene {@link SortField} array for sorting documents/rows according to the column family name.
     *
     * @return A Lucene {@link SortField} array for sorting documents/rows according to the column family name.
     */
    public List<SortField> sortFields() {
        return Collections.singletonList(new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed)
            throws IOException {
                return new FieldComparator.TermValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        CellName bb1 = clusteringKey(val1);
                        CellName bb2 = clusteringKey(val2);
                        return cellNameType.compare(bb1, bb2);
                    }
                };
            }
        }));
    }

    /**
     * Returns a Lucene {@link Query} array to retrieving documents/rows whose clustering key is between the two
     * specified column name prefixes.
     *
     * @param start The start key.
     * @param stop  The stop key.
     * @return A Lucene {@link Query} array to retrieving documents/rows whose clustering key is between the two
     * specified column name prefixes.
     */
    public Query query(Composite start, Composite stop) {
        return new ClusteringKeyQuery(start, stop, this);
    }

    /**
     * Returns the {@code String} human-readable representation of the specified cell name.
     *
     * @param cellName A cell name.
     * @return The {@code String} human-readable representation of the specified cell name.
     */
    public String toString(Composite cellName) {
        return ByteBufferUtils.toString(cellName.toByteBuffer(), cellNameType.asAbstractType());
    }

    /**
     * Returns a clustering key based {@link Row} {@link Comparator}.
     *
     * @return A clustering key based {@link Row} {@link Comparator}.
     */
    public Comparator<Row> comparator() {
        return new Comparator<Row>() {
            @Override
            public int compare(Row row1, Row row2) {
                CellNameType nameType = getType();
                CellName name1 = clusteringKey(row1);
                CellName name2 = clusteringKey(row2);
                return nameType.compare(name1, name2);
            }
        };
    }
}
