package com.stratio.cassandra.lucene.index;

import com.stratio.cassandra.lucene.IndexException;
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
public class DocumentIterator implements Iterator<Document> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentIterator.class);

    private IndexSearcher searcher;
    private Query query;
    private Sort sort;
    private ScoreDoc after;
    private Integer count;
    private Set<String> fields;
    private LinkedList<Document> documents = new LinkedList<>();
    private boolean mayHaveMore = true;

    public DocumentIterator(IndexSearcher searcher,
                            Query query,
                            Sort sort,
                            ScoreDoc after,
                            Integer count,
                            Set<String> fields) {
        this.searcher = searcher;
        this.query = query;
        this.sort = sort;
        this.after = after;
        this.count = count;
        this.fields = fields;
    }

    private void fetch() {
        try {

            // Search for top documents
            TopDocs topDocs = searcher.searchAfter(after, query, count, sort);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            logger.debug("Get page with {} documents", scoreDocs.length);

            // Check inf mayHaveMore
            mayHaveMore = scoreDocs.length == count;

            // Collect the documents from query result
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document document = searcher.doc(scoreDoc.doc, fields);
                documents.add(document);
                after = scoreDoc;
            }

        } catch (Exception e) {
            throw new IndexException(logger, e, "Error searching in with %s and %s", query, sort);
        }
    }

    /**
     * Returns {@code true} if the iteration has more {@link Document}s. (In other words, returns {@code true} if {@link
     * #next} would return an {@link Document} rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more{@link Document}s
     */
    public boolean hasNext() {
        if (mayHaveMore && documents.isEmpty()) {
            fetch();
        }
        return !documents.isEmpty();
    }

    /**
     * Returns the next {@link Document} in the iteration.
     *
     * @return the next {@link Document} in the iteration
     * @throws NoSuchElementException if the iteration has no more {@link Document}s
     */
    public Document next() {
        if (hasNext()) {
            return documents.poll();
        } else {
            throw new NoSuchElementException();
        }
    }

}
