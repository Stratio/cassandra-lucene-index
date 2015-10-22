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

import com.stratio.cassandra.lucene.service.RowService;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(Index.class);

    // Setup CQL query handler
    static {
        try {
            Field field = ClientState.class.getDeclaredField("cqlQueryHandler");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, new IndexQueryHandler());
        } catch (Exception e) {
            logger.error("Unable to set Lucene CQL query handler", e);
        }
    }

    private ColumnDefinition columnDefinition;
    private String indexName;
    private IndexConfig indexConfig;
    private String name;
    private RowService service;
    private boolean isExcluded;

    /**
     * Returns the indexed column definition.
     *
     * @return The indexed column definition.
     */
    public ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    /** {@inheritDoc} */
    @Override
    public String getIndexName() {
        return indexName;
    }

    /** {@inheritDoc} */
    @Override
    public void init() {
        logger.info("Initializing Lucene index");
        try {
            // Load column family info
            columnDefinition = columnDefs.iterator().next();
            indexName = columnDefinition.getIndexName();
            indexConfig = newIndexConfig();
            name = indexConfig.getName();
            service = RowService.build(baseCfs, indexConfig);
            logger.info("Initialized index {}", name);
            isExcluded = indexConfig.getExcludedDataCenters().contains(DatabaseDescriptor.getLocalDataCenter());
            if (isExcluded) {
                logger.info("All writes to this index will be ignored");
            }
        } catch (Exception e) {
            logger.error("Error initializing Lucene index " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void index(ByteBuffer key, ColumnFamily columnFamily) {
        if (!isExcluded) {
            logger.debug("Indexing row in Lucene index {}", name);
            try {
                long timestamp = System.currentTimeMillis();
                service.index(key, columnFamily, timestamp);
            } catch (Exception e) {
                logger.error("Error indexing row in Lucene index " + name, e);
            }
        } else {
            logger.debug("Ignoring excluded indexing in Lucene index {}", name);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void delete(DecoratedKey key, OpOrder.Group opGroup) {
        if (!isExcluded) {
            logger.debug("Removing row from Lucene index {}", name);
            try {
                service.delete(key);
                service = null;
            } catch (Exception e) {
                logger.error("Error deleting row in Lucene index " + name, e);
            }
        } else {
            logger.debug("Ignoring excluded deletion in Lucene index {}", name);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean indexes(CellName cellName) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void validateOptions() throws ConfigurationException {
        logger.debug("Validating Lucene index options");
        try {
            newIndexConfig();
            logger.debug("Lucene index options are valid");
        } catch (IndexException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long estimateResultRows() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnFamilyStore getIndexCfs() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void removeIndex(ByteBuffer columnName) {
        logger.info("Removing Lucene index {}", name);
        try {
            removeIndex();
            logger.info("Removed Lucene index {}", name);
        } catch (Exception e) {
            logger.error("Error removing Lucene index " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void invalidate() {
        logger.info("Invalidating Lucene index {}", name);
        try {
            removeIndex();
            logger.info("Invalidated Lucene index {}", name);
        } catch (Exception e) {
            logger.error("Error invalidating Lucene index " + name, e);
        }
    }

    private void removeIndex() {
        try {
            service.delete();
        } catch (Exception e) {
            logger.error("Error while removing index", e);
            FileUtils.deleteRecursive(indexConfig.getPath().toFile());
        }
    }

    private IndexConfig newIndexConfig() {
        ColumnDefinition cfDef = columnDefs.iterator().next();
        String ksName = cfDef.ksName;
        String cfName = cfDef.cfName;
        CFMetaData metadata = Schema.instance.getCFMetaData(ksName, cfName);
        return new IndexConfig(metadata, cfDef);
    }

    /** {@inheritDoc} */
    @Override
    public void truncateBlocking(long truncatedAt) {
        logger.info("Truncating Lucene index {}", name);
        try {
            service.truncate();
            logger.info("Truncated Lucene index {}", name);
        } catch (Exception e) {
            logger.error("Error truncating Lucene index " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void reload() {
    }

    /** {@inheritDoc} */
    @Override
    public void forceBlockingFlush() {
        logger.info("Flushing Lucene index {}", name);
        try {
            service.commit();
            logger.info("Flushed Lucene index {}", name);
        } catch (Exception e) {
            logger.error("Error flushing Lucene index " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        return new IndexSearcher(baseCfs.indexManager, this, columns, service);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return name;
    }
}
