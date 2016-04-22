/**
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

import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * {@link Iterator} for retrieving Lucene {@link Document}s satisfying a {@link Query} from an {@link IndexSearcher}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DocumentIterator implements CloseableIterator<Pair<Document, ScoreDoc>> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentIterator.class);

    private final FSIndex index;
    private final Query query;
    private final Integer page;
    private final Deque<Pair<Document, ScoreDoc>> documents = new LinkedList<>();
    private final Sort sort;
    private ScoreDoc after;
    private boolean mayHaveMore = true;

    /**
     * Builds a new iterator over the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param index the Lucene index
     * @param query the query to be satisfied by the documents
     * @param sort the sort in which the documents are going to be retrieved
     * @param after a pointer to the start document (not included)
     * @param page the iteration page size
     */
    DocumentIterator(FSIndex index, Query query, Sort sort, ScoreDoc after, Integer page) {
        this.index = index;
        this.query = query;
        this.sort = sort;
        this.after = after;
        this.page = page < Integer.MAX_VALUE ? page + 1 : page;
    }

    private void fetch() {

        TimeCounter time = TimeCounter.create().start();
        List<Pair<Document, ScoreDoc>> docs = index.doSearch(query, sort, after, page);
        logger.debug("Get page with {} documents in {}", docs.size(), time.stop());

        mayHaveMore = docs.size() == page;
        docs.forEach(pair -> {
            documents.add(pair);
            after = pair.right;
        });
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
        return mayHaveMore && documents.isEmpty();
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
    public void close() {
    }
}
