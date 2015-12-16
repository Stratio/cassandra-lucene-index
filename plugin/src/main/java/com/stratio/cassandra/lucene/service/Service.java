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

import com.stratio.cassandra.lucene.IndexConfig;
import com.stratio.cassandra.lucene.util.TaskQueue;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Class for providing operations between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Service {

    protected static final Logger logger = LoggerFactory.getLogger(Service.class);

    protected final ColumnFamilyStore columnFamilyStore;
    protected final CFMetaData tableMetadata;
    protected final IndexMetadata indexMetadata;
    protected final IndexConfig config;
    protected final LuceneIndex lucene;
    private final TaskQueue indexQueue;
    private final RowMapper rowMapper;

    /**
     * Constructor using the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     * @throws IOException If there are I/O errors.
     */
    public Service(IndexConfig config) throws IOException {
        this.config = config;
        columnFamilyStore = config.getColumnFamilyStore();
        tableMetadata = config.getTableMetadata();
        indexMetadata = config.getIndexMetadata();
        lucene = new LuceneIndex(config);

        int threads = config.getIndexingThreads();
        indexQueue = threads > 0 ? new TaskQueue(threads, config.getIndexingQueuesSize()) : null;

        rowMapper = new RowMapper(config);
    }

    public RowMapper getRowMapper() {
        return rowMapper;
    }

    /**
     * Commits the pending changes. This operation is performed asynchronously.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void commit() throws IOException {
        if (indexQueue != null) {
            indexQueue.await();
        }
        lucene.commit();
    }

    /**
     * Deletes all the index contents.
     *
     * @throws IOException If there are I/O errors.
     */
    public final void truncate() throws IOException {
        lucene.truncate();
    }

    /**
     * Closes and removes all the index files.
     */
    public final void remove() {
        try {
            if (indexQueue != null) {
                indexQueue.shutdown();
            }
            lucene.delete();
        } catch (Exception e) {
            logger.error("Error while removing index", e);
            FileUtils.deleteRecursive(config.getPath().toFile());
        }
    }

    private void run(Object id, Runnable task) {
        if (indexQueue == null) {
            task.run();
        } else {
            indexQueue.submitAsynchronous(id, task);
        }
    }

    public void insertRow(final DecoratedKey key, final Row row) {
        run(key, () -> {

        });
    }

}
