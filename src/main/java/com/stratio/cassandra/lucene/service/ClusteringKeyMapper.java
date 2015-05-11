/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;
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
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class only be used in column families
 * with wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyMapper {

    /** The Lucene field name */
    public static final String FIELD_NAME = "_clustering_key";

    /** The column family meta data */
    protected final CFMetaData metadata;

    /** The type of the clustering key, which is the type of the column names */
    protected final CellNameType cellNameType;

    /** The clustering key type as composite */
    protected final CompositeType compositeType;

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     */
    protected ClusteringKeyMapper(CFMetaData metadata) {
        this.metadata = metadata;
        this.cellNameType = metadata.comparator;
        this.compositeType = (CompositeType) cellNameType.asAbstractType();
    }

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
     */
    public static ClusteringKeyMapper instance(CFMetaData metadata) {
        return new ClusteringKeyMapper(metadata);
    }

    /**
     * Returns the clustering key validation type. It's always a {@link CompositeType} in CQL3 tables.
     *
     * @return The clustering key validation type.
     */
    public final CellNameType getType() {
        return cellNameType;
    }

    /**
     * Adds to the specified document the clustering key contained in the specified cell name.
     *
     * @param document The document where the clustering key is going to be added.
     * @param cellName A cell name containing the clustering key to be added.
     */
    public final void addFields(Document document, CellName cellName) {
        String serializedKey = ByteBufferUtils.toString(cellName.toByteBuffer());
        BytesRef bytesRef = new BytesRef(serializedKey);
        document.add(new StringField(FIELD_NAME, serializedKey, Field.Store.YES));
        document.add(new SortedDocValuesField(FIELD_NAME, bytesRef));
    }

    /**
     * Returns the first clustering key contained in the specified {@link ColumnFamily}. Note that there could be more
     * clustering keys in the column family.
     *
     * @param columnFamily A column family.
     * @return The first clustering key contained in the specified {@link ColumnFamily}.
     */
    public final CellName clusteringKey(ColumnFamily columnFamily) {
        Iterator<Cell> iterator = columnFamily.iterator();
        return iterator.hasNext() ? clusteringKey(iterator.next().name()) : null;
    }

    /**
     * Returns the common clustering keys of the specified column family.
     *
     * @param columnFamily A storage engine {@link ColumnFamily}.
     * @return The common clustering keys of the specified column family.
     */
    public final List<CellName> clusteringKeys(ColumnFamily columnFamily) {
        List<CellName> clusteringKeys = new ArrayList<>();
        CellName lastClusteringKey = null;
        for (Cell cell : columnFamily) {
            CellName cellName = cell.name();
            if (!isStatic(cellName)) {
                CellName clusteringKey = extractClusteringKey(cellName);
                if (lastClusteringKey == null || !lastClusteringKey.isSameCQL3RowAs(cellNameType, clusteringKey)) {
                    lastClusteringKey = clusteringKey;
                    clusteringKeys.add(clusteringKey);
                }
            }
        }
        return sort(clusteringKeys);
    }

    protected final CellName extractClusteringKey(CellName cellName) {
        int numClusteringColumns = metadata.clusteringColumns().size();
        ByteBuffer[] components = new ByteBuffer[numClusteringColumns + 1];
        for (int i = 0; i < numClusteringColumns; i++) {
            components[i] = cellName.get(i);
        }
        components[numClusteringColumns] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        return cellNameType.makeCellName((Object[]) components);
    }

    protected final boolean isStatic(CellName cellName) {
        int numClusteringColumns = metadata.clusteringColumns().size();
        for (int i = 0; i < numClusteringColumns; i++) {
            if (ByteBufferUtils.isEmpty(cellName.get(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the storage engine column name for the specified column identifier using the specified clustering key.
     *
     * @param cellName         The clustering key.
     * @param columnDefinition The column definition.
     * @return A storage engine column name.
     */
    public final CellName makeCellName(CellName cellName, ColumnDefinition columnDefinition) {
        return cellNameType.create(start(cellName), columnDefinition);
    }

    /**
     * Returns the first clustering key contained in the specified row.
     *
     * @param row A {@link Row}.
     * @return The first clustering key contained in the specified row.
     */
    public final CellName clusteringKey(Row row) {
        return clusteringKey(row.cf);
    }

    /**
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    public final CellName clusteringKey(Document document) {
        String string = document.get(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the clustering key contained in the specified Lucene field value.
     *
     * @param bytesRef The {@link BytesRef} containing the raw clustering key to be get.
     * @return The clustering key contained in the specified Lucene field value.
     */
    public final CellName clusteringKey(BytesRef bytesRef) {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the clustering key contained in the specified {@link CellName}.
     *
     * @param cellName A {@link CellName}.
     * @return The clustering key contained in the specified {@link CellName}.
     */
    public final CellName clusteringKey(CellName cellName) {
        CBuilder builder = cellNameType.builder();
        for (int i = 0; i < metadata.clusteringColumns().size(); i++) {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        Composite prefix = builder.build();
        return cellNameType.rowMarker(prefix);
    }

    /**
     * Returns the first possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    public final Composite start(CellName cellName) {
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
    public final Composite end(CellName cellName) {
        return start(cellName).withEOC(Composite.EOC.END);
    }

    public final Columns columns(Row row) {
        ColumnFamily columnFamily = row.cf;
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
                    columns.add(Column.fromDecomposed(name, value, valueType, false));
                }
            }
        }
        return columns;
    }

    public final Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily) {
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

    public final ColumnSlice[] columnSlices(List<CellName> clusteringKeys) {
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
    public final List<CellName> sort(List<CellName> clusteringKeys) {
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
    public SortField[] sortFields() {
        return new SortField[]{new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field,
                                                    int hits,
                                                    int sort,
                                                    boolean reversed) throws IOException {
                return new FieldComparator.TermOrdValComparator(hits, field, false) {
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        CellName bb1 = clusteringKey(val1);
                        CellName bb2 = clusteringKey(val2);
                        return cellNameType.compare(bb1, bb2);
                    }
                };
            }
        })};
    }

    /**
     * Returns a Lucene {@link Query} array to retrieving documents/rows whose clustering key is between the two
     * specified column name prefixes.
     *
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
    public final String toString(Composite cellName) {
        return ByteBufferUtils.toString(cellName.toByteBuffer(), cellNameType.asAbstractType());
    }

}
