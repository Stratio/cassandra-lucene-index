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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Class wrapping a Lucene file system-based directory and its readers, writers and searchers.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FSIndex {

    private static final Logger logger = LoggerFactory.getLogger(FSIndex.class);

    private final String name;
    private final Path path;
    private final Analyzer analyzer;
    private final double refresh;
    private final int ramBufferMB;
    private final int maxMergeMB;
    private final int maxCachedMB;

    private Sort mergeSort;
    private Set<String> fields;
    private Directory directory;
    private IndexWriter indexWriter;
    private SearcherManager searcherManager;
    private ControlledRealTimeReopenThread<IndexSearcher> searcherReopener;

    // Disable max boolean query clauses limit
    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     * Builds a new {@link FSIndex}.
     *
     * @param name the index name
     * @param path the directory path
     * @param analyzer the index writer analyzer
     * @param refresh the index reader refresh frequency in seconds
     * @param ramBufferMB the index writer RAM buffer size in MB
     * @param maxMergeMB the directory max merge size in MB
     * @param maxCachedMB the directory max cache size in MB
     */
    public FSIndex(String name,
                   Path path,
                   Analyzer analyzer,
                   double refresh,
                   int ramBufferMB,
                   int maxMergeMB,
                   int maxCachedMB) {
        this.name = name;
        this.path = path;
        this.analyzer = analyzer;
        this.refresh = refresh;
        this.ramBufferMB = ramBufferMB;
        this.maxMergeMB = maxMergeMB;
        this.maxCachedMB = maxCachedMB;
    }

    /**
     * Initializes this index with the specified merge sort and fields to be loaded.
     *
     * @param mergeSort the sort to be applied to the index during merges
     * @param fields the names of the document fields to be loaded
     */
    public void init(Sort mergeSort, Set<String> fields) {
        this.mergeSort = mergeSort;
        this.fields = fields;
        try {

            // Open or create directory
            FSDirectory fsDirectory = FSDirectory.open(path);
            directory = new NRTCachingDirectory(fsDirectory, maxMergeMB, maxCachedMB);

            TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
            SortingMergePolicy sortingMergePolicy = new SortingMergePolicy(tieredMergePolicy, mergeSort);

            // Setup index writer
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setRAMBufferSizeMB(ramBufferMB);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setUseCompoundFile(true);
            indexWriterConfig.setMergePolicy(sortingMergePolicy);
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

        } catch (Exception e) {
            throw new IndexException(logger, e, "Error while creating index {}", name);
        }
    }

    private <T> T doWithSearcher(CheckedFunction<IndexSearcher, T> function) throws IOException {
        IndexSearcher searcher = searcherManager.acquire();
        try {
            return function.apply(searcher);
        } finally {
            searcherManager.release(searcher);
        }
    }

    @FunctionalInterface
    private interface CheckedFunction<T, R> {
        R apply(T t) throws IOException;
    }

    /**
     * Upserts the specified {@link Document} by first deleting the documents containing {@code Term} and then adding
     * the new document. The delete and then add are atomic as seen by a reader on the same index (flush may happen only
     * after the add).
     *
     * @param term the {@link Term} to identify the document(s) to be deleted
     * @param document the {@link Document} to be added
     */
    public void upsert(Term term, Document document) {
        logger.debug("Indexing {} with term {} in {}", document, term, name);
        try {
            indexWriter.updateDocument(term, document);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error indexing {} with term {} in {}", document, term, name);
        }
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term the {@link Term} identifying the documents to be deleted
     */
    public void delete(Term term) {
        logger.debug("Deleting {} from {}", term, name);
        try {
            indexWriter.deleteDocuments(term);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error deleting {} from {}", term, name);
        }
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query the {@link Query} identifying the documents to be deleted
     */
    public void delete(Query query) {
        logger.debug("Deleting {} from {}", query, name);
        try {
            indexWriter.deleteDocuments(query);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error deleting {} from {}", query, name);
        }
    }

    /**
     * Deletes all the {@link Document}s.
     */
    public void truncate() {
        try {
            indexWriter.deleteAll();
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error truncating {}", name);
        }
        logger.info("Truncated {}", name);
    }

    /**
     * Commits the pending changes.
     */
    public void commit() {
        try {
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error committing {}", name);
        }
        logger.debug("Committed {}", name);
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
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error closing {}", name);
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
            logger.error(String.format("Error deleting %s", name), e);
        } finally {
            FileUtils.deleteRecursive(path.toFile());
        }
        logger.info("Deleted {}", name);
    }

    /**
     * Finds the top {@code count} hits for {@code query} and sorting the hits by {@code sort}.
     *
     * @param query the {@link Query} to search for
     * @param sort the {@link Sort} to be applied
     * @param after the starting {@link ScoreDoc}
     * @param count the max number of results to be collected
     * @return the found documents, sorted according to the supplied {@link Sort} instance
     */
    public DocumentIterator search(Query after, Query query, Sort sort, int count) {
        logger.debug("Searching in {}\n" +
                     "after: {}\n" +
                     "query: {}\n" +
                     " sort: {}\n" +
                     "count: {}", name, after, query, sort, count);
        return new DocumentIterator(searcherManager, mergeSort, after, query, sort, count, fields);
    }

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return the number of {@link Document}s
     */
    public int getNumDocs() {
        logger.debug("Getting {} num docs", name);
        try {
            return doWithSearcher(searcher -> searcher.getIndexReader().numDocs());
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error getting {} num docs", name);
        }
    }

    /**
     * Returns the total number of deleted {@link Document}s in this index.
     *
     * @return the number of deleted {@link Document}s
     */
    public int getNumDeletedDocs() {
        logger.debug("Getting {} num deleted docs", name);
        try {
            return doWithSearcher(searcher -> searcher.getIndexReader().numDeletedDocs());
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error getting {} num docs", name);
        }
    }

    /**
     * Optimizes the index forcing merge segments leaving the specified number of segments. This operation may block
     * until all merging completes.
     *
     * @param maxNumSegments the maximum number of segments left in the index after merging finishes
     * @param doWait {@code true} if the call should block until the operation completes
     */
    public void forceMerge(int maxNumSegments, boolean doWait) {
        logger.info("Merging {} segments to {}", name, maxNumSegments);
        try {
            indexWriter.forceMerge(maxNumSegments, doWait);
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error merging {} segments to {}", name, maxNumSegments);
        }
        logger.info("Merged {} segments to {}", name, maxNumSegments);
    }

    /**
     * Optimizes the index forcing merge of all segments that have deleted documents. This operation may block until all
     * merging completes.
     *
     * @param doWait {@code true} if the call should block until the operation completes
     */
    public void forceMergeDeletes(boolean doWait) {
        logger.info("Merging {} segments with deletions", name);
        try {
            indexWriter.forceMergeDeletes(doWait);
            indexWriter.commit();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error merging {} segments with deletion", name);
        }
        logger.info("Merged {} segments with deletions", name);
    }

    /**
     * Refreshes the index readers.
     */
    public void refresh() {
        logger.debug("Refreshing {} readers", name);
        try {
            searcherManager.maybeRefreshBlocking();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error refreshing {} readers", name);
        }
        logger.debug("Refreshed {} readers", name);
    }
}
