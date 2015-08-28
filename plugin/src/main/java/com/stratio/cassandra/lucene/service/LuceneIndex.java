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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private ObjectName objectName;

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     * Builds a new {@link LuceneIndex} using the specified {@link IndexConfig}.
     *
     * @param config The {@link IndexConfig}.
     * @throws IOException If Lucene throws IO errors.
     */
    public LuceneIndex(IndexConfig config) throws IOException {
        this.path = config.getPath();
        this.name = config.getName();

        // Open or create directory
        FSDirectory fsDirectory = FSDirectory.open(path);
        directory = new NRTCachingDirectory(fsDirectory, config.getMaxMergeMB(), config.getMaxCachedMB());

        // Setup index writer
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(config.getAnalyzer());
        indexWriterConfig.setRAMBufferSizeMB(config.getRamBufferMB());
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
        TrackingIndexWriter trackingIndexWriter = new TrackingIndexWriter(indexWriter);
        searcherManager = new SearcherManager(indexWriter, true, searcherFactory);
        searcherReopener = new ControlledRealTimeReopenThread<>(trackingIndexWriter,
                                                                searcherManager,
                                                                config.getRefreshSeconds(),
                                                                config.getRefreshSeconds());
        searcherReopener.start();

        // Register JMX MBean
        try {
            objectName = new ObjectName(String.format(
                    "com.stratio.cassandra.lucene:type=LuceneIndexes,keyspace=%s,table=%s,index=%s",
                    config.getKeyspaceName(),
                    config.getTableName(),
                    name));
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
        } catch (MBeanException | OperationsException e) {
            logger.error("Error while registering MBean", e);
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
        logger.debug("{} update document {} with term {}", name, document, term);
        indexWriter.updateDocument(term, document);
    }

    /**
     * Updates the specified {@link Document}s by first deleting the documents containing {@code Term} and then adding
     * the new document. The delete and then add are atomic as seen by a reader on the same index (flush may happen only
     * after the add).
     *
     * @param documents The {@link Document}s to be added.
     * @throws IOException If Lucene throws IO errors.
     */
    public void upsert(Map<Term, Document> documents) throws IOException {
        for (Map.Entry<Term, Document> entry : documents.entrySet()) {
            upsert(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Deletes all the {@link Document}s containing the specified {@link Term}.
     *
     * @param term The {@link Term} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Term term) throws IOException {
        logger.debug("{} delete by term {}", name, term);
        indexWriter.deleteDocuments(term);
    }

    /**
     * Deletes all the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param query The {@link Query} to identify the documents to be deleted.
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete(Query query) throws IOException {
        logger.debug("{} deleting by query {}", name, query);
        indexWriter.deleteDocuments(query);
    }

    /**
     * Deletes all the {@link Document}s.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void truncate() throws IOException {
        indexWriter.deleteAll();
        logger.info("{} truncated", name);
    }

    /**
     * Commits the pending changes.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public void commit() throws IOException {
        indexWriter.commit();
        logger.info("{} committed", name);
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
            logger.error("Error while removing MBean", e);
        }
        logger.info("{} closed", name);
    }

    /**
     * Closes the index and removes all its files.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    public void delete() throws IOException {
        close();
        FileUtils.deleteRecursive(path.toFile());
        logger.info("{} removed", name);
    }

    public SearcherManager getSearcherManager() {
        return searcherManager;
    }

    /**
     * Finds the top {@code count} hits for {@code query}, applying {@code clusteringKeyFilter} if non-null and sorting
     * the hits by {@code sort}.
     *
     * @param searcher The {@link IndexSearcher} to be used.
     * @param query    The {@link Query} to search for.
     * @param sort     The {@link Sort} to be applied.
     * @param after    The starting {@link ScoreDoc}.
     * @param count    The max number of results to be collected.
     * @param fields   The names of the fields to be loaded.
     * @return The found documents, sorted according to the supplied {@link Sort} instance.
     * @throws IOException If Lucene throws IO errors.
     */
    public Map<Document, ScoreDoc> search(IndexSearcher searcher,
                                          Query query,
                                          Sort sort,
                                          ScoreDoc after,
                                          Integer count,
                                          Set<String> fields) throws IOException {

        // Search for top documents
        TopDocs topDocs = searcher.searchAfter(after, query, count, sort);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        // Collect the documents from query result
        LinkedHashMap<Document, ScoreDoc> searchResults = new LinkedHashMap<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document document = searcher.doc(scoreDoc.doc, fields);
            searchResults.put(document, scoreDoc);
        }

        return searchResults;
    }

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return The total number of {@link Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    @Override
    public long getNumDocs() throws IOException {
        logger.debug("{} get num docs", name);
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
        logger.debug("{} get num deleted docs", name);
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
        logger.info("{} merging index segments to {}", name, maxNumSegments);
        indexWriter.forceMerge(maxNumSegments, doWait);
        indexWriter.commit();
        logger.info("{} segments merge completed", name);
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
        logger.info("{} merging index segments with deletions", name);
        indexWriter.forceMergeDeletes(doWait);
        indexWriter.commit();
        logger.info("{} merging index segments with deletions completed", name);
    }

    /**
     * Refreshes the index readers.
     */
    @Override
    public void refresh() throws IOException {
        logger.info("{} refreshing readers", name);
        commit();
        searcherManager.maybeRefreshBlocking();
    }
}
