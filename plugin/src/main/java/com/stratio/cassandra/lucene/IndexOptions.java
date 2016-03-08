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
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.schema.IndexMetadata;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lucene {@link Index} user-specified configuration options parser.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class IndexOptions {

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

    public static final String TOKEN_RANGE_CACHE_SIZE_OPTION = "token_range_cache_size";
    public static final int DEFAULT_TOKEN_RANGE_CACHE_SIZE = 16;

    public static final String SEARCH_CACHE_SIZE_OPTION = "search_cache_size";
    public static final int DEFAULT_SEARCH_CACHE_SIZE = 16;

    public static final String DIRECTORY_PATH_OPTION = "directory_path";
    public static final String INDEXES_DIR_NAME = "lucene";

    public static final String SCHEMA_OPTION = "schema";

    /** The mapping schema */
    public final Schema schema;

    /** The path of the directory where the index files will be stored */
    public final Path path;

    /** The Lucene index searcher refresh frequency, in seconds */
    public final double refreshSeconds;

    /** The Lucene's max RAM buffer size, in MB */
    public final int ramBufferMB;

    /** The Lucene's max segments merge size size, in MB */
    public final int maxMergeMB;

    /** The Lucene's max cache size, in MB */
    public final int maxCachedMB;

    /** The number of asynchronous indexing threads */
    public final int indexingThreads;

    /** The size of the asynchronous indexing queues */
    public final int indexingQueuesSize;

    /** The max size of the token range cache */
    public final int tokenRangeCacheSize;

    /** The max size of the search cache */
    public final int searchCacheSize;

    /** The names of the data centers excluded from indexing */
    public final List<String> excludedDataCenters;

    /**
     * Builds a new {@link IndexOptions} for the column family and index metadata.
     *
     * @param tableMetadata the indexed table metadata
     * @param indexMetadata the index metadata
     */
    public IndexOptions(CFMetaData tableMetadata, IndexMetadata indexMetadata) {
        Map<String, String> options = indexMetadata.options;
        refreshSeconds = parseRefresh(options);
        ramBufferMB = parseRamBufferMB(options);
        maxMergeMB = parseMaxMergeMB(options);
        maxCachedMB = parseMaxCachedMB(options);
        indexingThreads = parseIndexingThreads(options);
        indexingQueuesSize = parseIndexingQueuesSize(options);
        excludedDataCenters = parseExcludedDataCenters(options);
        tokenRangeCacheSize = parseTokenRangeCacheSize(options);
        searchCacheSize = parseSearchCacheSize(options);
        path = parsePath(options, tableMetadata, indexMetadata);
        schema = parseSchema(options, tableMetadata);
    }

    /**
     * Validates the specified index options.
     *
     * @param options the options to be validated
     * @param metadata the indexed table metadata
     */
    public static void validateOptions(Map<String, String> options, CFMetaData metadata) {
        parseRefresh(options);
        parseRamBufferMB(options);
        parseMaxMergeMB(options);
        parseMaxCachedMB(options);
        parseIndexingThreads(options);
        parseIndexingQueuesSize(options);
        parseExcludedDataCenters(options);
        parseTokenRangeCacheSize(options);
        parseSearchCacheSize(options);
        parseSchema(options, metadata);
        parsePath(options, metadata, null);
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

    private static int parseTokenRangeCacheSize(Map<String, String> options) {
        String tokenRangeCacheSizeOption = options.get(TOKEN_RANGE_CACHE_SIZE_OPTION);
        if (tokenRangeCacheSizeOption != null) {
            int tokenRangeCacheSize;
            try {
                tokenRangeCacheSize = Integer.parseInt(tokenRangeCacheSizeOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a positive integer", TOKEN_RANGE_CACHE_SIZE_OPTION);
            }
            if (tokenRangeCacheSize < 0) {
                throw new IndexException("'%s' must be positive", TOKEN_RANGE_CACHE_SIZE_OPTION);
            }
            return tokenRangeCacheSize;
        } else {
            return DEFAULT_TOKEN_RANGE_CACHE_SIZE;
        }
    }

    private static int parseSearchCacheSize(Map<String, String> options) {
        String searchCacheSizeOption = options.get(SEARCH_CACHE_SIZE_OPTION);
        if (searchCacheSizeOption != null) {
            int searchCacheSize;
            try {
                searchCacheSize = Integer.parseInt(searchCacheSizeOption);
            } catch (NumberFormatException e) {
                throw new IndexException("'%s' must be a positive integer", SEARCH_CACHE_SIZE_OPTION);
            }
            if (searchCacheSize < 0) {
                throw new IndexException("'%s' must be positive", SEARCH_CACHE_SIZE_OPTION);
            }
            return searchCacheSize;
        } else {
            return DEFAULT_SEARCH_CACHE_SIZE;
        }
    }

    private static Path parsePath(Map<String, String> options, CFMetaData tableMetadata, IndexMetadata indexMetadata) {
        String pathOption = options.get(DIRECTORY_PATH_OPTION);
        if (pathOption != null) {
            return Paths.get(pathOption);
        } else if (indexMetadata != null) {
            Directories directories = new Directories(tableMetadata);
            String basePath = directories.getDirectoryForNewSSTables().getAbsolutePath();
            return Paths.get(basePath + File.separator + INDEXES_DIR_NAME + File.separator + indexMetadata.name);
        }
        return null;
    }

    private static Schema parseSchema(Map<String, String> options, CFMetaData tableMetadata) {
        String schemaOption = options.get(SCHEMA_OPTION);
        if (schemaOption != null && !schemaOption.trim().isEmpty()) {
            Schema schema;
            try {
                schema = SchemaBuilder.fromJson(schemaOption).build();
                schema.validate(tableMetadata);
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
                          .add("tokenRangeCacheSize", tokenRangeCacheSize)
                          .add("searchCacheSize", searchCacheSize)
                          .add("path", path)
                          .add("schema", schema)
                          .toString();
    }
}
