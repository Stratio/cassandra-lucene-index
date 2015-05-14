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
package com.stratio.cassandra.lucene;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * The Stratio Lucene index user-specified configuration.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class IndexConfig {

    public static final String SCHEMA_OPTION = "schema";

    public static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
    public static final double DEFAULT_REFRESH_SECONDS = 60;

    public static final String DIRECTORY_PATH_OPTION = "directory_path";
    public static final String INDEXES_DIR_NAME = "lucene";

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

    private final Schema schema;
    private final double refreshSeconds;
    private final Path path;
    private final int ramBufferMB;
    private final int maxMergeMB;
    private final int maxCachedMB;
    private final int indexingThreads;
    private final int indexingQueuesSize;

    /**
     * Builds a new {@link IndexConfig} for the column family defined by the specified metadata using the specified
     * index options.
     *
     * @param metadata The metadata of the indexed column family.
     * @param options  The index options.
     */
    public IndexConfig(CFMetaData metadata, Map<String, String> options) {
        refreshSeconds = parseRefresh(options);
        ramBufferMB = parseRamBufferMB(options);
        maxMergeMB = parseMaxMergeMB(options);
        maxCachedMB = parseMaxCachedMB(options);
        indexingThreads = parseIndexingThreads(options);
        indexingQueuesSize = parseIndexingQueuesSize(options);
        schema = parseSchema(options, metadata);
        path = parsePath(options, metadata);
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
     * Returns the path of the directory where the Lucene files will be stored. This directory is collocated to the
     * indexed column family one.
     *
     * @return The path where the Lucene files will be stored.
     */
    public Path getPath() {
        return path;
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

    public int getMaxMergeMB() {
        return maxMergeMB;
    }

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
        double refreshSeconds;
        if (refreshOption != null) {
            try {
                refreshSeconds = Double.parseDouble(refreshOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s' must be a strictly positive double", REFRESH_SECONDS_OPTION);
                throw new RuntimeException(msg);
            }
            if (refreshSeconds <= 0) {
                String msg = String.format("'%s' must be strictly positive", REFRESH_SECONDS_OPTION);
                throw new RuntimeException(msg);
            } else {
                return refreshSeconds;
            }
        } else {
            return DEFAULT_REFRESH_SECONDS;
        }
    }

    private static int parseRamBufferMB(Map<String, String> options) {
        String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
        int ramBufferMB;
        if (ramBufferSizeOption != null) {
            try {
                ramBufferMB = Integer.parseInt(ramBufferSizeOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (ramBufferMB <= 0) {
                String msg = String.format("'%s' must be strictly positive", RAM_BUFFER_MB_OPTION);
                throw new RuntimeException(msg);
            }
            return ramBufferMB;
        } else {
            return DEFAULT_RAM_BUFFER_MB;
        }
    }

    private static int parseMaxMergeMB(Map<String, String> options) {
        String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
        int maxMergeMB;
        if (maxMergeSizeMBOption != null) {
            try {
                maxMergeMB = Integer.parseInt(maxMergeSizeMBOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s' must be a strictly positive integer", MAX_MERGE_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (maxMergeMB <= 0) {
                String msg = String.format("'%s' must be strictly positive", MAX_MERGE_MB_OPTION);
                throw new RuntimeException(msg);
            }
            return maxMergeMB;
        } else {
            return DEFAULT_MAX_MERGE_MB;
        }
    }

    private static int parseMaxCachedMB(Map<String, String> options) {
        String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
        int maxCachedMB;
        if (maxCachedMBOption != null) {
            try {
                maxCachedMB = Integer.parseInt(maxCachedMBOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s'  must be a strictly positive integer", MAX_CACHED_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (maxCachedMB <= 0) {
                String msg = String.format("'%s'  must be strictly positive", MAX_CACHED_MB_OPTION);
                throw new RuntimeException(msg);
            }
            return maxCachedMB;
        } else {
            return DEFAULT_MAX_CACHED_MB;
        }
    }

    private static int parseIndexingThreads(Map<String, String> options) {
        String indexPoolNumQueuesOption = options.get(INDEXING_THREADS_OPTION);
        int indexingThreads;
        if (indexPoolNumQueuesOption != null) {
            try {
                indexingThreads = Integer.parseInt(indexPoolNumQueuesOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s'  must be a positive integer", INDEXING_THREADS_OPTION);
                throw new RuntimeException(msg);
            }
            return indexingThreads;
        } else {
            return DEFAULT_INDEXING_THREADS;
        }
    }

    private static int parseIndexingQueuesSize(Map<String, String> options) {
        String indexPoolQueuesSizeOption = options.get(INDEXING_QUEUES_SIZE_OPTION);
        int indexingQueuesSize;
        if (indexPoolQueuesSizeOption != null) {
            try {
                indexingQueuesSize = Integer.parseInt(indexPoolQueuesSizeOption);
            } catch (NumberFormatException e) {
                String msg = String.format("'%s'  must be a strictly positive integer", INDEXING_QUEUES_SIZE_OPTION);
                throw new RuntimeException(msg);
            }
            if (indexingQueuesSize <= 0) {
                String msg = String.format("'%s'  must be strictly positive", INDEXING_QUEUES_SIZE_OPTION);
                throw new RuntimeException(msg);
            }
            return indexingQueuesSize;
        } else {
            return DEFAULT_INDEXING_QUEUES_SIZE;
        }
    }

    private static Schema parseSchema(Map<String, String> options, CFMetaData metadata) {
        String schemaOption = options.get(SCHEMA_OPTION);
        Schema schema;
        if (schemaOption != null && !schemaOption.trim().isEmpty()) {
            try {
                schema = Schema.fromJson(schemaOption);
                schema.validate(metadata);
            } catch (Exception e) {
                String msg = String.format("'%s' is invalid : %s", SCHEMA_OPTION, e.getMessage());
                throw new RuntimeException(msg);
            }
            return schema;
        } else {
            String msg = String.format("'%s' required", SCHEMA_OPTION);
            throw new RuntimeException(msg);
        }
    }

    private static Path parsePath(Map<String, String> options, CFMetaData metadata) {
        String pathOption = options.get(DIRECTORY_PATH_OPTION);
        if (pathOption == null) {
            String pathString = DatabaseDescriptor.getAllDataFileLocations()[0] +
                                File.separatorChar +
                                metadata.ksName +
                                File.separatorChar +
                                metadata.cfName +
                                "-" +
                                metadata.cfId +
                                File.separatorChar +
                                INDEXES_DIR_NAME;
            return Paths.get(pathString);
        } else {
            return Paths.get(pathOption);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("schema", schema)
                      .add("refreshSeconds", refreshSeconds)
                      .add("path", path)
                      .add("ramBufferMB", ramBufferMB)
                      .add("maxMergeMB", maxMergeMB)
                      .add("maxCachedMB", maxCachedMB)
                      .add("indexingThreads", indexingThreads)
                      .add("indexingQueuesSize", indexingQueuesSize)
                      .toString();
    }
}
