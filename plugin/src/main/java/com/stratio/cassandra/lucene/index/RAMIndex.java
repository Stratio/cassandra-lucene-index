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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class wrapping a Lucene RAM directory and its readers, writers and searchers for NRT.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RAMIndex {

    private static final Logger logger = LoggerFactory.getLogger(RAMIndex.class);

    private final Directory directory;
    private final IndexWriter indexWriter;

    /**
     * Builds a new {@link RAMIndex}.
     *
     * @param analyzer The index writer analyzer.
     */
    public RAMIndex(Analyzer analyzer) {
        try {
            directory = new RAMDirectory();
            indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IndexException(logger, e, "Error while creating index");
        }
    }

    /**
     * Updates the specified {@link Document}
     *
     * @param document The {@link Document} to be added.
     */
    public void add(Document document) {
        try {
            indexWriter.addDocument(document);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error while indexing %s", document);
        }
    }

    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all associated resources.
     */
    public void close() {
        try {
            indexWriter.close();
            directory.close();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error while closing");
        }
    }

    /**
     * Finds the top {@code count} hits for {@code query} and sorting the hits by {@code sort}.
     *
     * @param query The {@link Query} to search for.
     * @param sort The {@link Sort} to be applied.
     * @param count The max number of results to be collected.
     * @param fields The names of the fields to be loaded.
     * @return The found documents, sorted according to the supplied {@link Sort} instance.
     */
    public List<Document> search(Query query, Sort sort, Integer count, Set<String> fields) {
        try {
            indexWriter.commit();
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(query, count, sort);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            List<Document> documents = new ArrayList<>(count);
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document document = searcher.doc(scoreDoc.doc, fields);
                documents.add(document);
            }
            searcher.getIndexReader().close();
            return documents;
        } catch (IOException e) {
            throw new IndexException(logger, e, "Error while searching");
        }
    }
}
