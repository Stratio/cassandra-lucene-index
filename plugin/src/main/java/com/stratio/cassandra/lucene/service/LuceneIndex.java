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

import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.OperationsException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class wrapping a Lucene directory and its readers, writers and searchers for NRT.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneIndex implements LuceneIndexMBean {

    private final String keyspace;
    private final String table;
    private final String name;
    private final Path path;
    private final Double refreshSeconds;
    private final Integer ramBufferMB;
    private final Integer maxMergeMB;
    private final Integer maxCachedMB;
    private final Analyzer analyzer;
    private final String logName;

    private Directory directory;
    private IndexWriter indexWriter;
    private SearcherManager searcherManager;
    private ControlledRealTimeReopenThread<IndexSearcher> searcherReopener;
    private SortingMergePolicy sortingMergePolicy;

    private Sort sort;
    private ObjectName objectName;

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     * Builds a new {@code RowDirectory} using the specified directory path and analyzer.
     *
     * @param keyspace       The keyspace name.
     * @param table          The table name.
     * @param name           The index name.
     * @param path           The analyzer to be used. The path of the directory in where the Lucene files will be
     *                       stored.
     * @param refreshSeconds The index readers refresh time in seconds. No guarantees that the writings are visible
     *                       until this time.
     * @param ramBufferMB    The index writer buffer size in MB.
     * @param maxMergeMB     NRTCachingDirectory max merge size in MB.
     * @param maxCachedMB    NRTCachingDirectory max cached MB.
     * @param analyzer       The default {@link Analyzer}.
     */
    public LuceneIndex(String keyspace,
                       String table,
                       String name,
                       Path path,
                       Double refreshSeconds,
                       Integer ramBufferMB,
                       Integer maxMergeMB,
                       Integer maxCachedMB,
                       Analyzer analyzer) {
        this.keyspace = keyspace;
        this.table = table;
        this.name = name;
        this.path = path;
        this.refreshSeconds = refreshSeconds;
        this.ramBufferMB = ramBufferMB;
        this.maxMergeMB = maxMergeMB;
        this.maxCachedMB = maxCachedMB;
        this.analyzer = analyzer;
        this.logName = String.format("Lucene index %s.%s.%s", keyspace, table, name);
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

        // Register JMX MBean
        try {
            objectName = new ObjectName(String.format(
                    "com.stratio.cassandra.lucene:type=LuceneIndexes,keyspace=%s,table=%s,index=%s",
                    keyspace,
                    table,
                    name));
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
        } catch (MBeanException | OperationsException e) {
            Log.error(e, "Error while registering MBean");
        }
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
        Log.debug("%s update document %s with term %s", logName, document, term);
        indexWriter.updateDocument(term, document);
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term The {@link Term} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Term term) throws IOException {
        Log.debug(String.format("%s delete by term %s", logName, term));
        indexWriter.deleteDocuments(term);
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query The {@link Query} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Query query) throws IOException {
        Log.debug("%s deleting by query %s", logName, query);
        indexWriter.deleteDocuments(query);
    }

    /**
     * Deletes all the {@link Document}s.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void truncate() throws IOException {
        indexWriter.deleteAll();
        Log.info("%s truncated", logName);
    }

    /**
     * Commits the pending changes.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public void commit() throws IOException {
        indexWriter.commit();
        Log.info("%s committed", logName);
    }

    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all associated resources.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void close() throws IOException {
        searcherReopener.interrupt();
        searcherManager.close();
        indexWriter.close();
        directory.close();
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName);
        } catch (MBeanException | OperationsException e) {
            Log.error(e, "Error while removing MBean");
        }
        Log.info("%s closed", logName);
    }

    /**
     * Closes the index and removes all its files.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete() throws IOException {
        close();
        FileUtils.deleteRecursive(path.toFile());
        Log.info("%s removed", logName);
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
        Log.debug("%s search by query %s", logName, query);
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
    @Override
    public long getNumDocs() throws IOException {
        Log.debug("%s get num docs", logName);
        IndexSearcher searcher = searcherManager.acquire();
        try {
            return searcher.getIndexReader().numDocs();
        } finally {
            searcherManager.release(searcher);
        }
    }

    /**
     * Returns the total number of deleted {@link Document}s in this index.
     *
     * @return The total number of deleted {@link Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public long getNumDeletedDocs() throws IOException {
        Log.debug("%s get num deleted docs", logName);
        IndexSearcher searcher = searcherManager.acquire();
        try {
            return searcher.getIndexReader().numDeletedDocs();
        } finally {
            searcherManager.release(searcher);
        }
    }

    /**
     * Optimizes the index forcing merge segments leaving the specified number of segments. This operation may block
     * until all merging completes.
     *
     * @param maxNumSegments The maximum number of segments left in the index after merging finishes.
     * @param doWait         {@code true} if the call should block until the operation completes.
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        Log.info("%s merging index segments to %d", logName, maxNumSegments);
        indexWriter.forceMerge(maxNumSegments, doWait);
        indexWriter.commit();
        Log.info("%s segments merge completed", logName);
    }

    /**
     * Optimizes the index forcing merge of all segments that have deleted documents.. This operation may block until
     * all merging completes.
     *
     * @param doWait {@code true} if the call should block until the operation completes.
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public void forceMergeDeletes(boolean doWait) throws IOException {
        Log.info("%s merging index segments with deletions", logName);
        indexWriter.forceMergeDeletes(doWait);
        indexWriter.commit();
        Log.info("%s merging index segments with deletions completed", logName);
    }

    /**
     * Refreshes the index readers.
     */
    @Override
    public void refresh() {
        Log.info("%s refreshing readers", logName);
        searcherReopener.run();
    }
}
