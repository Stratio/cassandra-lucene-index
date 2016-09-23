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
import com.stratio.cassandra.lucene.util.TimeCounter;
import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * {@link Iterator} for retrieving Lucene {@link Document}s satisfying a {@link Query} from an {@link IndexSearcher}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DocumentIterator implements CloseableIterator<Pair<Document, ScoreDoc>> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentIterator.class);

    /** The max number of rows to be read per iteration. */
    static final int MAX_PAGE_SIZE = 10000;

    private final SearcherManager manager;
    private final Query query;
    final int page;
    private final Deque<Pair<Document, ScoreDoc>> documents = new LinkedList<>();
    private final Sort sort, indexSort;
    private final Set<String> fields;
    private ScoreDoc after = null;
    private boolean finished = false;
    private IndexSearcher searcher;
    private int numReadDocuments = 0;
    private final Query startQuery;

    /**
     * Builds a new iterator over the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param manager the Lucene index searcher manager
     * @param query the query to be satisfied by the documents
     * @param sort the sort in which the documents are going to be retrieved
     * @param page the iteration page size
     * @param fields the names of the document fields to be loaded
     */
    DocumentIterator(SearcherManager manager,
                     Sort indexSort,
                     Query after,
                     Query query,
                     Sort sort,
                     int page,
                     Set<String> fields) {
        this.manager = manager;
        this.query = query;
        this.indexSort = indexSort;
        this.fields = fields;
        this.startQuery = after;
        this.page = Math.min(page, MAX_PAGE_SIZE) + 1;
        TimeCounter time = TimeCounter.create().start();
        try {
            searcher = manager.acquire();
            this.sort = sort.rewrite(searcher);
            if (after != null) {
                Tracer.trace("Searching for last Lucene index position");
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(after, BooleanClause.Occur.FILTER);
                builder.add(query, BooleanClause.Occur.MUST);
                ScoreDoc[] scoreDocs = searcher.search(builder.build(), 1, this.sort).scoreDocs;
                if (scoreDocs.length > 0) {
                    this.after = scoreDocs[0];
                }
            }
        } catch (IOException e) {
            throw new IndexException(e, "Error while acquiring index searcher");
        } finally {
            logger.debug("Index query initialized in {}", time.stop());
        }
    }

    private synchronized void fetch() {

        try {

            TimeCounter time = TimeCounter.create().start();

            TopDocs topDocs;
            if (startQuery == null && EarlyTerminatingSortingCollector.canEarlyTerminate(sort, indexSort)) {
                FieldDoc fieldDoc = after == null ? null : (FieldDoc) after;
                TopFieldCollector collector = TopFieldCollector.create(sort, page, fieldDoc, true, false, false);
                int hits = numReadDocuments + page;
                searcher.search(query, new EarlyTerminatingSortingCollector(collector, sort, hits, indexSort));
                topDocs = collector.topDocs();
            } else {
                topDocs = searcher.searchAfter(after, query, page, sort);
            }

            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            numReadDocuments += scoreDocs.length;
            finished = scoreDocs.length < page;
            for (ScoreDoc scoreDoc : scoreDocs) {
                after = scoreDoc;
                Document document = searcher.doc(scoreDoc.doc, fields);
                documents.add(Pair.create(document, scoreDoc));
            }

            Tracer.trace("Lucene index fetches {} documents", scoreDocs.length);
            logger.debug("Index query page fetched with {} documents in {}", scoreDocs.length, time.stop());

        } catch (Exception e) {
            close();
            throw new IndexException(logger, e, "Error searching in with {} and {}", query, sort);
        }

        if (finished) {
            close();
        }
    }

    /**
     * Returns {@code true} if the iteration has more {@link Document}s. (In other words, returns {@code true} if {@link
     * #next} would return an {@link Document} rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more {@link Document}s
     */
    @Override
    public boolean hasNext() {
        if (needsFetch()) {
            fetch();
        }
        return !documents.isEmpty();
    }

    /**
     * Returns if more {@link Document}s should be fetched from the Lucene index.
     *
     * @return {@code true} if more documents should be fetched, {@code false} otherwise
     */
    public boolean needsFetch() {
        return !finished && documents.isEmpty();
    }

    /**
     * Returns the next {@link Document} in the iteration.
     *
     * @return the next document
     * @throws NoSuchElementException if the iteration has no more {@link Document}s
     */
    @Override
    public Pair<Document, ScoreDoc> next() {
        if (hasNext()) {
            return documents.poll();
        } else {
            throw new NoSuchElementException();
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() {
        if (searcher != null) {
            try {
                manager.release(searcher);
            } catch (IOException e) {
                throw new IndexException("Error while releasing index searcher");
            } finally {
                searcher = null;
            }
        }
    }
}
