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

package com.stratio.cassandra.lucene;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.service.RowService;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.LuceneQueryHandler;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A {@link PerRowSecondaryIndex} that uses Apache Lucene as backend. It allows, among others, multi-column and
 * full-text search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Index extends PerRowSecondaryIndex {

    // Setup CQL query handler
    static {
        try {
            Field field = ClientState.class.getDeclaredField("cqlQueryHandler");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, new LuceneQueryHandler());
        } catch (Exception e) {
            Log.error("Unable to set Lucene CQL query handler");
        }
    }

    private SecondaryIndexManager secondaryIndexManager;
    private ColumnDefinition columnDefinition;

    private String keyspaceName;
    private String tableName;
    private String indexName;
    private String columnName;
    private String logName;

    private RowService rowService;

    @Override
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the indexed keyspace name.
     *
     * @return The indexed keyspace name.
     */
    public String getKeyspaceName() {
        return keyspaceName;
    }

    /**
     * Returns the indexed table name.
     *
     * @return The indexed table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the indexed column name.
     *
     * @return The indexed column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the indexed column definition.
     *
     * @return The indexed column definition.
     */
    public ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    @Override
    public void init() {
        Log.info("Initializing Lucene index");
        try {
            // Load column family info
            secondaryIndexManager = baseCfs.indexManager;
            columnDefinition = columnDefs.iterator().next();
            indexName = columnDefinition.getIndexName();
            keyspaceName = baseCfs.metadata.ksName;
            tableName = baseCfs.metadata.cfName;
            columnName = columnDefinition.name.toString();
            logName = String.format("%s.%s.%s", keyspaceName, tableName, indexName);

            // Build row mapper
            rowService = RowService.build(baseCfs, columnDefinition);

            Log.info("Initialized index %s", logName);
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while initializing Lucene index %s", logName);
        }
    }

    /**
     * Index the given row.
     *
     * @param key          The partition key.
     * @param columnFamily The column family data to be indexed
     */
    @Override
    public void index(ByteBuffer key, ColumnFamily columnFamily) {
        Log.debug("Indexing row %s in Lucene index %s", key, logName);
        try {
            if (rowService != null) {
                long timestamp = System.currentTimeMillis();
                rowService.index(key, columnFamily, timestamp);
            }
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while indexing row %s in Lucene index %s", key, logName);
        }
    }

    /**
     * Cleans up deleted rows from cassandra cleanup compaction.
     *
     * @param key The partition key of the physical row to be deleted.
     */
    @Override
    public void delete(DecoratedKey key, OpOrder.Group opGroup) {
        Log.debug("Removing row %s from Lucene index %s", key, logName);
        try {
            rowService.delete(key);
            rowService = null;
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error deleting row %s", key);
        }
    }

    @Override
    public boolean indexes(CellName cellName) {
        return true;
    }

    @Override
    public void validateOptions() throws ConfigurationException {
        Log.debug("Validating Lucene index options");
        try {
            ColumnDefinition indexedColumnDefinition = columnDefs.iterator().next();
            String ksName = indexedColumnDefinition.ksName;
            String cfName = indexedColumnDefinition.cfName;
            CFMetaData metadata = Schema.instance.getCFMetaData(ksName, cfName);
            new IndexConfig(metadata, indexedColumnDefinition);
            Log.debug("Lucene index options are valid");
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            String message = "Error while validating Lucene index options: " + e.getMessage();
            Log.error(e, message);
            throw new ConfigurationException(message, e);
        }
    }

    @Override
    public long estimateResultRows() {
        return 1;
    }

    @Override
    public ColumnFamilyStore getIndexCfs() {
        return null;
    }

    @Override
    public void removeIndex(ByteBuffer columnName) {
        Log.info("Removing Lucene index %s", logName);
        try {
            if (rowService != null) {
                rowService.delete();
                rowService = null;
            }
            Log.info("Removed Lucene index %s", logName);
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while removing Lucene index %s", logName);
        }
    }

    @Override
    public void invalidate() {
        Log.info("Invalidating Lucene index %s", logName);
        try {
            if (rowService != null) {
                rowService.delete();
                rowService = null;
            }
            Log.info("Invalidated Lucene index %s", logName);
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while invalidating Lucene index %s", logName);
        }
    }

    @Override
    public void truncateBlocking(long truncatedAt) {
        Log.info("Truncating Lucene index %s", logName);
        try {
            if (rowService != null) {
                rowService.truncate();
            }
            Log.info("Truncated Lucene index %s", logName);
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while truncating Lucene index %s", logName);
        }
    }

    @Override
    public void reload() {
    }

    @Override
    public void forceBlockingFlush() {
        Log.info("Flushing Lucene index %s", logName);
        try {
            rowService.commit();
            Log.info("Flushed Lucene index %s", logName);
        } catch (IndexException e) {
            throw e.log();
        } catch (Exception e) {
            throw new IndexException(e, "Error while flushing Lucene index %s", logName);
        }
    }

    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        return new IndexSearcher(secondaryIndexManager, this, columns, rowService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("indexName", indexName)
                      .add("keyspaceName", keyspaceName)
                      .add("tableName", tableName)
                      .add("columnName", columnName)
                      .toString();
    }
}
