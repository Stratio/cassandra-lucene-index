package com.stratio.cassandra.lucene.index;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * {@link Iterator} for retrieving Lucene {@link Document}s satisfying a {@link Query} from an {@link IndexSearcher}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DocumentIterator implements CloseableIterator<Document> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentIterator.class);

    private SearcherManager manager;
    private IndexSearcher searcher;
    private Query query;
    private Sort sort;
    private ScoreDoc after;
    private Integer page;
    private Set<String> fields;
    private LinkedList<Document> documents = new LinkedList<>();
    private boolean mayHaveMore = true;

    /**
     * Builds a new iterator over the {@link Document}s satisfying the specified {@link Query}.
     *
     * @param manager the index searcher manager
     * @param query the query to be satisfied by the documents
     * @param sort the sort in which the documents are going to be retrieved
     * @param after a pointer to the start document (not included)
     * @param limit the max number of documents to be retrieved
     * @param fields the names of the fields to be loaded
     */
    DocumentIterator(SearcherManager manager,
                     Query query,
                     Sort sort,
                     ScoreDoc after,
                     Integer limit,
                     Set<String> fields) {
        this.manager = manager;
        this.query = query;
        this.sort = sort;
        this.after = after;
        this.page = limit < Integer.MAX_VALUE ? limit + 1 : limit;
        this.fields = fields;
        try {
            searcher = manager.acquire();
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error acquiring index searcher");
        }
    }

    private void fetch() {
        try {
            TimeCounter time = TimeCounter.create().start();

            // Search for top documents
            sort = sort.rewrite(searcher);
            TopDocs topDocs = searcher.searchAfter(after, query, page, sort);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;

            // Check inf mayHaveMore
            mayHaveMore = scoreDocs.length == page;

            // Collect the documents from query result
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document document = searcher.doc(scoreDoc.doc, fields);
                documents.add(document);
                after = scoreDoc;
            }

            logger.debug("Get page with {} documents in {}", scoreDocs.length, time.stop());

        } catch (Exception e) {
            throw new IndexException(logger, e, "Error searching in with %s and %s", query, sort);
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
    public Document next() {
        if (hasNext()) {
            return documents.poll();
        } else {
            throw new NoSuchElementException();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            manager.release(searcher);
        } catch (Exception e) {
            throw new IndexException(logger, e, "Error releasing index searcher");
        }
    }
}
