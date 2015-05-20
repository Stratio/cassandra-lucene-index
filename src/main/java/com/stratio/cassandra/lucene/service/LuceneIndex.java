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

import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortingMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class wrapping a Lucene directory and its readers , writers and searchers for NRT.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneIndex {

    private final Path path;
    private final Double refreshSeconds;
    private final Integer ramBufferMB;
    private final Integer maxMergeMB;
    private final Integer maxCachedMB;
    private final Analyzer analyzer;

    private Directory directory;
    private IndexWriter indexWriter;
    private SearcherManager searcherManager;
    private ControlledRealTimeReopenThread<IndexSearcher> searcherReopener;
    private SortingMergePolicy sortingMergePolicy;

    private Sort sort;

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     * Builds a new {@code RowDirectory} using the specified directory path and analyzer.
     *
     * @param path           The analyzer to be used. The path of the directory in where the Lucene files will be
     *                       stored.
     * @param refreshSeconds The index readers refresh time in seconds. No guarantees that the writings are visible
     *                       until this time.
     * @param ramBufferMB    The index writer buffer size in MB.
     * @param maxMergeMB     NRTCachingDirectory max merge size in MB.
     * @param maxCachedMB    NRTCachingDirectory max cached MB.
     * @param analyzer       The default {@link Analyzer}.
     */
    public LuceneIndex(Path path,
                       Double refreshSeconds,
                       Integer ramBufferMB,
                       Integer maxMergeMB,
                       Integer maxCachedMB,
                       Analyzer analyzer) {
        this.path = path;
        this.refreshSeconds = refreshSeconds;
        this.ramBufferMB = ramBufferMB;
        this.maxMergeMB = maxMergeMB;
        this.maxCachedMB = maxCachedMB;
        this.analyzer = analyzer;
    }

    /**
     * Initializes this using the specified {@link Sort} for trying to keep the {@link Document}s sorted.
     *
     * @param sort The {@link Sort} to be used.
     */
    public void init(Sort sort) {
        Log.debug("Initializing index");
        try {
            this.sort = sort;

            // Open or create directory
            FSDirectory fsDirectory = FSDirectory.open(path);
            directory = new NRTCachingDirectory(fsDirectory, maxMergeMB, maxCachedMB);

            sortingMergePolicy = new SortingMergePolicy(new TieredMergePolicy(), sort);

            // Setup index writer
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setRAMBufferSizeMB(ramBufferMB);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            config.setUseCompoundFile(true);
            config.setMergePolicy(sortingMergePolicy);
            indexWriter = new IndexWriter(directory, config);

            // Setup NRT search
            SearcherFactory searcherFactory = new SearcherFactory() {
                public IndexSearcher newSearcher(IndexReader reader) throws IOException {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new NoIDFSimilarity());
                    return searcher;
                }
            };
            TrackingIndexWriter trackingIndexWriter = new TrackingIndexWriter(indexWriter);
            searcherManager = new SearcherManager(indexWriter, true, searcherFactory);
            searcherReopener = new ControlledRealTimeReopenThread<>(trackingIndexWriter,
                                                                    searcherManager,
                                                                    refreshSeconds,
                                                                    refreshSeconds);
            searcherReopener.start(); // Start the refresher thread
        } catch (IOException e) {
            Log.error(e, "Error while initializing index");
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates the specified {@link Document} by first deleting the documents containing {@code Term} and then adding
     * the new document. The delete and then add are atomic as seen by a reader on the same index (flush may happen only
     * after the add).
     *
     * @param term     The {@link Term} to identify the document(s) to be deleted.
     * @param document The {@link Document} to be added.
     */
    public void upsert(Term term, Document document) {
        Log.debug("Updating document %s with term %s", document, term);
        try {
            indexWriter.updateDocument(term, document);
        } catch (IOException e) {
            Log.error(e, "Error while updating document %s with term %s", document, term);
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term The {@link Term} to identify the documents to be deleted.
     */
    public void delete(Term term) {
        Log.debug(String.format("Deleting by term %s", term));
        try {
            indexWriter.deleteDocuments(term);
        } catch (IOException e) {
            Log.error(e, "Error while deleting by term %s", term);
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query The {@link Query} to identify the documents to be deleted.
     */
    public void delete(Query query) {
        Log.debug("Deleting by query %s", query);
        try {
            indexWriter.deleteDocuments(query);
        } catch (IOException e) {
            Log.error(e, "Error while deleting by query %s", query);
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes all the {@link Document}s.
     */
    public void truncate() {
        Log.info("Truncating index");
        try {
            indexWriter.deleteAll();
        } catch (IOException e) {
            Log.error(e, "Error while truncating index");
            throw new RuntimeException(e);
        }
    }

    /**
     * Commits the pending changes.
     */
    public void commit() {
        Log.info("Committing");
        try {
            indexWriter.commit();
        } catch (IOException e) {
            Log.error(e, "Error while committing");
            throw new RuntimeException(e);
        }
    }

    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all associated resources.
     */
    public void close() {
        Log.info("Closing index");
        try {
            Log.info("Closing");
            searcherReopener.interrupt();
            searcherManager.close();
            indexWriter.close();
            directory.close();
        } catch (IOException e) {
            Log.error(e, "Error while closing index");
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the index and removes all its files.
     */
    public void delete() {
        Log.info("Removing");
        close();
        FileUtils.deleteRecursive(path.toFile());
    }

    /**
     * Finds the top {@code count} hits for {@code query}, applying {@code clusteringKeyFilter} if non-null, and sorting
     * the hits by the criteria in {@code sortFields}.
     *
     * @param query        The {@link Query} to search for.
     * @param sort         The {@link Sort} to be applied.
     * @param after        The starting {@link SearchResult}.
     * @param count        Return only the top {@code count} results.
     * @param fieldsToLoad The name of the fields to be loaded.
     * @return The found documents, sorted according to the supplied {@link Sort} instance.
     */
    public Map<Document, ScoreDoc> search(Query query,
                                          Sort sort,
                                          SearchResult after,
                                          Integer count,
                                          Set<String> fieldsToLoad,
                                          boolean usesRelevance) {
        Log.debug("Searching by query %s", query);
        try {
            IndexSearcher searcher = searcherManager.acquire();
            try {
                // Search
                ScoreDoc start = after == null ? null : after.getScoreDoc();
                TopDocs topDocs = topDocs(searcher, query, sort, start, count, usesRelevance);
                ScoreDoc[] scoreDocs = topDocs.scoreDocs;

                // Collect the documents from query result
                Map<Document, ScoreDoc> searchResults = new LinkedHashMap<>();
                for (ScoreDoc scoreDoc : scoreDocs) {
                    Document document = searcher.doc(scoreDoc.doc, fieldsToLoad);
                    searchResults.put(document, scoreDoc);
                }

                return searchResults;
            } finally {
                searcherManager.release(searcher);
            }
        } catch (IOException e) {
            Log.error(e, "Error while searching by query %s", query);
            throw new RuntimeException(e);
        }
    }

    private TopDocs topDocs(IndexSearcher searcher,
                            Query query,
                            Sort sort,
                            ScoreDoc after,
                            int count,
                            boolean usesRelevance) throws IOException {
        if (sort == null) {
            if (!usesRelevance) {
                FieldDoc start = after == null ? null : (FieldDoc) after;
                TopFieldCollector tfc = TopFieldCollector.create(this.sort, count, start, true, false, false);
                Collector collector = new EarlyTerminatingSortingCollector(tfc, this.sort, count, sortingMergePolicy);
                searcher.search(query, collector);
                return tfc.topDocs();
            } else {
                return searcher.searchAfter(after, query, count);
            }
        } else {
            return searcher.searchAfter(after, query, count, sort);
        }
    }

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return The total number of {@link Document}s in this index.
     */
    public long getNumDocs() {
        Log.debug("Getting num docs");
        try {
            IndexSearcher searcher = searcherManager.acquire();
            try {
                return searcher.getIndexReader().numDocs();
            } finally {
                searcherManager.release(searcher);
            }
        } catch (IOException e) {
            Log.error(e, "Error while getting num docs");
            throw new RuntimeException(e);
        }

    }
}
