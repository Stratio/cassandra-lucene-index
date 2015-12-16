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

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The Stratio Lucene index user-specified configuration.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexConfig {

    private static final Logger logger = LoggerFactory.getLogger(IndexConfig.class);

    public static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
    public static final double DEFAULT_REFRESH_SECONDS = 60;

    public static final String RAM_BUFFER_MB_OPTION = "ram_buffer_mb";
    public static final int DEFAULT_RAM_BUFFER_MB = 64;

    public static final String MAX_MERGE_MB_OPTION = "max_merge_mb";
    public static final int DEFAULT_MAX_MERGE_MB = 5;

    public static final String MAX_CACHED_MB_OPTION = "max_cached_mb";
    public static final int DEFAULT_MAX_CACHED_MB = 30;

    public static final String INDEXING_THREADS_OPTION = "indexing_threads";
    public static final int DEFAULT_INDEXING_THREADS = 0;

    public static final String INDEXING_QUEUES_SIZE_OPTION = "indexing_queues_size";
    public static final int DEFAULT_INDEXING_QUEUES_SIZE = 50;

    public static final String EXCLUDED_DATA_CENTERS_OPTION = "excluded_data_centers";
    public static final List<String> DEFAULT_EXCLUDED_DATA_CENTERS = Collections.emptyList();

    public static final String DIRECTORY_PATH_OPTION = "directory_path";
    public static final String INDEXES_DIR_NAME = "lucene";

    public static final String SCHEMA_OPTION = "schema";

    private final ColumnFamilyStore columnFamilyStore;
    private final CFMetaData tableMetadata;
    private final IndexMetadata indexMetadata;
    private Schema schema;
    private double refreshSeconds = DEFAULT_REFRESH_SECONDS;
    private Path path;
    private int ramBufferMB = DEFAULT_RAM_BUFFER_MB;
    private int maxMergeMB = DEFAULT_MAX_MERGE_MB;
    private int maxCachedMB = DEFAULT_MAX_CACHED_MB;
    private int indexingThreads = DEFAULT_INDEXING_THREADS;
    private int indexingQueuesSize = DEFAULT_INDEXING_QUEUES_SIZE;
    private List<String> excludedDataCenters = DEFAULT_EXCLUDED_DATA_CENTERS;

    /**
     * Builds a new {@link IndexConfig} for the column family and index metadata.
     *
     * @param columnFamilyStore The indexed column family.
     * @param indexMetadata The index metadata.
     */
    public IndexConfig(ColumnFamilyStore columnFamilyStore, IndexMetadata indexMetadata) {
        this.columnFamilyStore = columnFamilyStore;
        this.indexMetadata = indexMetadata;
        tableMetadata = columnFamilyStore.metadata;
        Map<String, String> options = indexMetadata.options;
        refreshSeconds = parseRefresh(options);
        ramBufferMB = parseRamBufferMB(options);
        maxMergeMB = parseMaxMergeMB(options);
        maxCachedMB = parseMaxCachedMB(options);
        indexingThreads = parseIndexingThreads(options);
        indexingQueuesSize = parseIndexingQueuesSize(options);
        excludedDataCenters = parseExcludedDataCenters(options);
        path = parsePath(options, tableMetadata);
        schema = parseSchema(options, tableMetadata);
    }

    /**
     * Validates the specified index options.
     *
     * @param options The options to be validated.
     */
    public static void validateOptions(Map<String, String> options) {
        parseRefresh(options);
        parseRamBufferMB(options);
        parseMaxMergeMB(options);
        parseMaxCachedMB(options);
        parseIndexingThreads(options);
        parseIndexingQueuesSize(options);
        parseExcludedDataCenters(options);
        parseSchema(options, null); // TODO: This should be mandatory, check Index#validateOptions
        parsePath(options, null); // TODO: This should be mandatory, check Index#validateOptions
    }

    /**
     * Returns the {@link ColumnFamilyStore} to be used.
     *
     * @return The {@link ColumnFamilyStore} to be used.
     */
    public ColumnFamilyStore getColumnFamilyStore() {
        return columnFamilyStore;
    }

    /**
     * Returns the {@link CFMetaData} to be used.
     *
     * @return The {@link CFMetaData} to be used.
     */
    public CFMetaData getTableMetadata() {
        return tableMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} to be used.
     *
     * @return The {@link IndexMetadata} to be used.
     */
    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    /**
     * Returns the name of the keyspace to be used.
     *
     * @return The name of the keyspace to be used.
     */
    public String getKeyspaceName() {
        return tableMetadata.ksName;
    }

    /**
     * Returns the name of the table to be used.
     *
     * @return The name of the table to be used.
     */
    public String getTableName() {
        return tableMetadata.cfName;
    }

    /**
     * Returns the name of the index to be used.
     *
     * @return The name of the index to be used.
     */
    public String getIndexName() {
        return indexMetadata.name;
    }

    /**
     * Returns the full qualified name of the index.
     *
     * @return The full qualified name of the index.
     */
    public String getName() {
        return String.format("%s.%s.%s", getKeyspaceName(), getTableName(), getIndexName());
    }

    /**
     * Returns {@code true} if the index uses wide rows, {@code false} otherwise.
     *
     * @return {@code true} if the index uses wide rows, {@code false} otherwise.
     */
    public boolean isWide() {
        return tableMetadata.clusteringColumns().size() > 0;
    }

    /**
     * Returns the {@link Schema} to be used.
     *
     * @return The {@link Schema} to be used.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the {@link Analyzer} to be used.
     *
     * @return The {@link Analyzer} to be used.
     */
    public Analyzer getAnalyzer() {
        return schema.getAnalyzer();
    }

    /**
     * Returns the path of the directory where the Lucene files will be stored. This directory is collocated to the
     * indexed column family one.
     *
     * @return The path where the Lucene files will be stored.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Returns the list of excluded data centers.
     *
     * @return The list of excluded data centers.
     */
    public List<String> getExcludedDataCenters() {
        return excludedDataCenters;
    }

    /**
     * Returns the number of seconds before refreshing the index readers.
     *
     * @return The number of seconds before refreshing the index readers.
     */
    public double getRefreshSeconds() {
        return refreshSeconds;
    }

    /**
     * Returns the size of the Lucene index writer write buffer. Its content will be committed to disk when full.
     *
     * @return The size of the write buffer.
     */
    public int getRamBufferMB() {
        return ramBufferMB;
    }

    /**
     * Returns the Lucene's max merge MBs.
     *
     * @return The Lucene's max merge MBs.
     */
    public int getMaxMergeMB() {
        return maxMergeMB;
    }

    /**
     * Returns the Lucene's max cached MBs.
     *
     * @return The Lucene's max cached MBs.
     */
    public int getMaxCachedMB() {
        return maxCachedMB;
    }

    /**
     * Returns the number of asynchronous indexing threads, where {@code 0} means synchronous indexing.
     *
     * @return The number of asynchronous indexing threads.
     */
    public int getIndexingThreads() {
        return indexingThreads;
    }

    /**
     * Returns the max number of queued documents per asynchronous indexing thread.
     *
     * @return The max number of queued documents per asynchronous indexing thread.
     */
    public int getIndexingQueuesSize() {
        return indexingQueuesSize;
    }

    private static double parseRefresh(Map<String, String> options) {
        String refreshOption = options.get(REFRESH_SECONDS_OPTION);
        if (refreshOption != null) {
            double refreshSeconds;
            try {
                refreshSeconds = Double.parseDouble(refreshOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a strictly positive double", REFRESH_SECONDS_OPTION);
            }
            if (refreshSeconds <= 0) {
                throw new IndexException("'%s' must be strictly positive", REFRESH_SECONDS_OPTION);
            }
            return refreshSeconds;
        } else {
            return DEFAULT_REFRESH_SECONDS;
        }
    }

    private static int parseRamBufferMB(Map<String, String> options) {
        String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
        if (ramBufferSizeOption != null) {
            int ramBufferMB;
            try {
                ramBufferMB = Integer.parseInt(ramBufferSizeOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
            }
            if (ramBufferMB <= 0) {
                throw new IndexException("'%s' must be strictly positive", RAM_BUFFER_MB_OPTION);
            }
            return ramBufferMB;
        } else {
            return DEFAULT_RAM_BUFFER_MB;
        }
    }

    private static int parseMaxMergeMB(Map<String, String> options) {
        String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
        if (maxMergeSizeMBOption != null) {
            int maxMergeMB;
            try {
                maxMergeMB = Integer.parseInt(maxMergeSizeMBOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a strictly positive integer", MAX_MERGE_MB_OPTION);
            }
            if (maxMergeMB <= 0) {
                throw new IndexException("'%s' must be strictly positive", MAX_MERGE_MB_OPTION);
            }
            return maxMergeMB;
        } else {
            return DEFAULT_MAX_MERGE_MB;
        }
    }

    private static int parseMaxCachedMB(Map<String, String> options) {
        String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
        if (maxCachedMBOption != null) {
            int maxCachedMB;
            try {
                maxCachedMB = Integer.parseInt(maxCachedMBOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a strictly positive integer", MAX_CACHED_MB_OPTION);
            }
            if (maxCachedMB <= 0) {
                throw new IndexException("'%s' must be strictly positive", MAX_CACHED_MB_OPTION);
            }
            return maxCachedMB;
        } else {
            return DEFAULT_MAX_CACHED_MB;
        }
    }

    private static int parseIndexingThreads(Map<String, String> options) {
        String indexPoolNumQueuesOption = options.get(INDEXING_THREADS_OPTION);
        if (indexPoolNumQueuesOption != null) {
            try {
                return Integer.parseInt(indexPoolNumQueuesOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a positive integer", INDEXING_THREADS_OPTION);
            }
        } else {
            return DEFAULT_INDEXING_THREADS;
        }
    }

    private static int parseIndexingQueuesSize(Map<String, String> options) {
        String indexPoolQueuesSizeOption = options.get(INDEXING_QUEUES_SIZE_OPTION);
        if (indexPoolQueuesSizeOption != null) {
            int indexingQueuesSize;
            try {
                indexingQueuesSize = Integer.parseInt(indexPoolQueuesSizeOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a strictly positive integer", INDEXING_QUEUES_SIZE_OPTION);
            }
            if (indexingQueuesSize <= 0) {
                throw new IndexException("'%s' must be strictly positive", INDEXING_QUEUES_SIZE_OPTION);
            }
            return indexingQueuesSize;
        } else {
            return DEFAULT_INDEXING_QUEUES_SIZE;
        }
    }

    private static List<String> parseExcludedDataCenters(Map<String, String> options) {
        String excludedDataCentersOption = options.get(EXCLUDED_DATA_CENTERS_OPTION);
        if (excludedDataCentersOption != null) {
            String[] array = excludedDataCentersOption.trim().split(",");
            return Arrays.asList(array);
        } else {
            return DEFAULT_EXCLUDED_DATA_CENTERS;
        }
    }

    private static Path parsePath(Map<String, String> options, CFMetaData tableMetadata) {
        String pathOption = options.get(DIRECTORY_PATH_OPTION);
        if (pathOption != null) {
            return Paths.get(pathOption);
        } else if (tableMetadata == null) { // TODO: This should be mandatory, check Index#validateOptions
            return null;
        } else {
            Directories directories = new Directories(tableMetadata);
            String basePath = directories.getDirectoryForNewSSTables().getAbsolutePath();
            return Paths.get(basePath + File.separator + INDEXES_DIR_NAME);
        }
    }

    private static Schema parseSchema(Map<String, String> options, CFMetaData tableMetadata) {
        String schemaOption = options.get(SCHEMA_OPTION);
        if (schemaOption != null && !schemaOption.trim().isEmpty()) {
            Schema schema;
            try {
                schema = SchemaBuilder.fromJson(schemaOption).build();
                if (tableMetadata != null) { // TODO: This should be mandatory, check Index#validateOptions
                    schema.validate(tableMetadata);
                }
                return schema;
            } catch (Exception e) {
                throw new IndexException(e, "'%s' is invalid : %s", SCHEMA_OPTION, e.getMessage());
            }
        } else {
            throw new IndexException("'%s' required", SCHEMA_OPTION);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("refreshSeconds", refreshSeconds)
                          .add("ramBufferMB", ramBufferMB)
                          .add("maxMergeMB", maxMergeMB)
                          .add("maxCachedMB", maxCachedMB)
                          .add("indexingThreads", indexingThreads)
                          .add("indexingQueuesSize", indexingQueuesSize)
                          .add("excludedDataCenters", excludedDataCenters)
                          .add("path", path)
                          .add("schema", schema)
                          .toString();
    }
}
