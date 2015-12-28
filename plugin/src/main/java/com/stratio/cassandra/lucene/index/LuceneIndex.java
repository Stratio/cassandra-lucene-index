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

package com.stratio.cassandra.lucene.index;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Class wrapping a Lucene directory and its readers, writers and searchers for NRT.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneIndex implements LuceneIndexMBean {

    private static final Logger logger = LoggerFactory.getLogger(LuceneIndex.class);

    private final Path path;
    private final String name;

    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;
    private final ControlledRealTimeReopenThread<IndexSearcher> searcherReopener;

    private final ObjectName mbean;

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     * Builds a new {@link LuceneIndex}.
     *
     * @param mbeanName The JMX MBean object name.
     * @param name The index name.
     * @param path The directory path.
     * @param analyzer The index writer analyzer.
     * @param refresh The index reader refresh frequency in seconds.
     * @param ramBufferMB The index writer RAM buffer size in MB.
     * @param maxMergeMB The directory max merge size in MB.
     * @param maxCachedMB The directory max cache size in MB.
     */
    public LuceneIndex(String mbeanName,
                       String name,
                       Path path,
                       Analyzer analyzer,
                       double refresh,
                       int ramBufferMB,
                       int maxMergeMB,
                       int maxCachedMB) {
        try {
            this.path = path;
            this.name = name;

            // Open or create directory
            FSDirectory fsDirectory = FSDirectory.open(path);
            directory = new NRTCachingDirectory(fsDirectory, maxMergeMB, maxCachedMB);

            // Setup index writer
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setRAMBufferSizeMB(ramBufferMB);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setUseCompoundFile(true);
            indexWriterConfig.setMergePolicy(new TieredMergePolicy());
            indexWriter = new IndexWriter(directory, indexWriterConfig);

            // Setup NRT search
            SearcherFactory searcherFactory = new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new NoIDFSimilarity());
                    return searcher;
                }
            };
            TrackingIndexWriter trackingWriter = new TrackingIndexWriter(indexWriter);
            searcherManager = new SearcherManager(indexWriter, true, searcherFactory);
            searcherReopener = new ControlledRealTimeReopenThread<>(trackingWriter, searcherManager, refresh, refresh);
            searcherReopener.start();

            // Register JMX MBean
            this.mbean = new ObjectName(mbeanName);
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, this.mbean);

        } catch (Exception e) {
            e.printStackTrace();
            throw new IndexException(logger, e, "Error while creating index %s", name);
        }
    }

    /**
     * Updates the specified {@link Document} by first deleting the documents containing {@code Term} and then adding
     * the new document. The delete and then add are atomic as seen by a reader on the same index (flush may happen only
     * after the add).
     *
     * @param term The {@link Term} to identify the document(s) to be deleted.
     * @param document The {@link Document} to be added.
     */
    public void upsert(Term term, Document document) {
        logger.debug("Indexing {} with term {} in {}", document, term, name);
        try {
            indexWriter.updateDocument(term, document);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error indexing %s with term %s in %s", document, term, name);
        }
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term The {@link Term} to identify the documents to be deleted.
     */
    public void delete(Term term) {
        logger.debug("Deleting {} from {}", term, name);
        try {
            indexWriter.deleteDocuments(term);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error deleting %s from %s", term, name);
        }
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query The {@link Query} to identify the documents to be deleted.
     */
    public void delete(Query query) {
        logger.debug("Deleting {} from {}", query, name);
        try {
            indexWriter.deleteDocuments(query);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error deleting %s from %s", query, name);
        }
    }

    /**
     * Deletes all the {@link Document}s.
     */
    public void truncate() {
        try {
            indexWriter.deleteAll();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error truncating %s", name);
        }
        logger.info("Truncated {}", name);
    }

    /**
     * Commits the pending changes.
     */
    @Override
    public void commit() {
        try {
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error committing %s", name);
        }
        logger.info("Committed {}", name);
    }

    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all associated resources.
     */
    public void close() {
        try {
            searcherReopener.interrupt();
            searcherManager.close();
            indexWriter.close();
            directory.close();
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbean);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error closing %s", name);
        }
        logger.info("Closed {}", name);
    }

    /**
     * Closes the index and removes all its files.
     */
    public void delete() {
        try {
            close();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error deleting %s", name);
        } finally {
            FileUtils.deleteRecursive(path.toFile());
        }
        logger.info("Deleted {}", name);
    }

    public SearcherManager getSearcherManager() {
        return searcherManager;
    }

    public IndexSearcher acquireSearcher() {
        try {
            return searcherManager.acquire();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error acquiring index searcher in %s", name);
        }
    }

    public void releaseSearcher(IndexSearcher indexSearcher) {
        try {
            searcherManager.release(indexSearcher);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error releasing index searcher in %s", name);
        }
    }

    /**
     * Finds the top {@code count} hits for {@code query} and sorting the hits by {@code sort}.
     *
     * @param searcher The {@link IndexSearcher} to be used.
     * @param query The {@link Query} to search for.
     * @param sort The {@link Sort} to be applied.
     * @param after The starting {@link ScoreDoc}.
     * @param count The max number of results to be collected.
     * @param fields The names of the fields to be loaded.
     * @return The found documents, sorted according to the supplied {@link Sort} instance.
     */
    public Iterator<Document> search(IndexSearcher searcher,
                                 Query query,
                                 Sort sort,
                                 ScoreDoc after,
                                 Integer count,
                                 Set<String> fields) {
        logger.debug("Searching in {} with {} and {}", name, query, sort);
        return new DocumentIterator(searcher, query, sort, after, count, fields);
    }

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return The total number of {@link Document}s in this index.
     */
    @Override
    public long getNumDocs() {
        logger.debug("Getting {} num docs", name);
        try {
            IndexSearcher searcher = searcherManager.acquire();
            try {
                return searcher.getIndexReader().numDocs();
            } finally {
                searcherManager.release(searcher);
            }
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error getting %s num docs", name);
        }
    }

    /**
     * Returns the total number of deleted {@link Document}s in this index.
     *
     * @return The total number of deleted {@link Document}s in this index.
     */
    @Override
    public long getNumDeletedDocs() {
        logger.debug("Getting %s num deleted docs", name);
        try {
            IndexSearcher searcher = searcherManager.acquire();
            try {
                return searcher.getIndexReader().numDeletedDocs();
            } finally {
                searcherManager.release(searcher);
            }
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error getting %s num docs", name);
        }
    }

    /**
     * Optimizes the index forcing merge segments leaving the specified number of segments. This operation may block
     * until all merging completes.
     *
     * @param maxNumSegments The maximum number of segments left in the index after merging finishes.
     * @param doWait {@code true} if the call should block until the operation completes.
     */
    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) {
        logger.info("Merging {} segments to {}", name, maxNumSegments);
        try {
            indexWriter.forceMerge(maxNumSegments, doWait);
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error merging %s segments to %s", name, maxNumSegments);
        }
        logger.info("Merged {} segments to {}", name, maxNumSegments);
    }

    /**
     * Optimizes the index forcing merge of all segments that have deleted documents.. This operation may block until
     * all merging completes.
     *
     * @param doWait {@code true} if the call should block until the operation completes.
     */
    @Override
    public void forceMergeDeletes(boolean doWait) {
        logger.info("Merging {} segments with deletions", name);
        try {
            indexWriter.forceMergeDeletes(doWait);
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error merging %s segments with deletion", name);
        }
        logger.info("Merged {} segments with deletions", name);
    }

    /**
     * Refreshes the index readers.
     */
    @Override
    public void refresh() {
        logger.info("Refreshing {} readers", name);
        try {
            commit();
            searcherManager.maybeRefreshBlocking();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error refreshing %s readers", name);
        }
        logger.info("Refreshed %s readers", name);
    }
}
