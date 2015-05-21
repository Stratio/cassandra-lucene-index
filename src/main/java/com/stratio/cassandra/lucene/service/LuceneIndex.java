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
 * Class wrapping a Lucene directory and its readers, writers and searchers for NRT.
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
     * @throws IOException If Lucene throws IO errors.
     */
    public void init(Sort sort) throws IOException {
        Log.debug("Initializing index");
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
    }

    /**
     * Updates the specified {@link Document} by first deleting the documents containing {@code Term} and then adding
     * the new document. The delete and then add are atomic as seen by a reader on the same index (flush may happen only
     * after the add).
     *
     * @param term     The {@link Term} to identify the document(s) to be deleted.
     * @param document The {@link Document} to be added.
     * @throws IOException If Lucene throws IO errors.
     */
    public void upsert(Term term, Document document) throws IOException {
        Log.debug("Updating document %s with term %s", document, term);
        indexWriter.updateDocument(term, document);
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term The {@link Term} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Term term) throws IOException {
        Log.debug(String.format("Deleting by term %s", term));
        indexWriter.deleteDocuments(term);
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query The {@link Query} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Query query) throws IOException {
        Log.debug("Deleting by query %s", query);
        indexWriter.deleteDocuments(query);
    }

    /**
     * Deletes all the {@link Document}s.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void truncate() throws IOException {
        Log.info("Truncating index");
        indexWriter.deleteAll();
    }

    /**
     * Commits the pending changes.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void commit() throws IOException {
        Log.info("Committing");
        indexWriter.commit();
    }

    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all associated resources.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void close() throws IOException {
        Log.info("Closing index");
        searcherReopener.interrupt();
        searcherManager.close();
        indexWriter.close();
        directory.close();
    }

    /**
     * Closes the index and removes all its files.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete() throws IOException {
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
     * @throws IOException If Lucene throws IO errors.
     */
    public Map<Document, ScoreDoc> search(Query query,
                                          Sort sort,
                                          ScoreDoc after,
                                          Integer count,
                                          Set<String> fieldsToLoad,
                                          boolean usesRelevance) throws IOException {
        Log.debug("Searching by query %s", query);
        IndexSearcher searcher = searcherManager.acquire();
        try {
            // Search
            ScoreDoc start = after == null ? null : after;
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
    }

    private TopDocs topDocs(IndexSearcher searcher,
                            Query query,
                            Sort sort,
                            ScoreDoc after,
                            int count,
                            boolean usesRelevance) throws IOException {
        if (sort != null) {
            return searcher.searchAfter(after, query, count, sort);
        } else if (usesRelevance) {
            return searcher.searchAfter(after, query, count);
        } else {
            FieldDoc start = after == null ? null : (FieldDoc) after;
            TopFieldCollector tfc = TopFieldCollector.create(this.sort, count, start, true, false, false);
            Collector collector = new EarlyTerminatingSortingCollector(tfc, this.sort, count, sortingMergePolicy);
            searcher.search(query, collector);
            return tfc.topDocs();
        }
    }

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return The total number of {@link Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    public long getNumDocs() throws IOException {
        Log.debug("Getting num docs");
        IndexSearcher searcher = searcherManager.acquire();
        try {
            return searcher.getIndexReader().numDocs();
        } finally {
            searcherManager.release(searcher);
        }
    }
}
