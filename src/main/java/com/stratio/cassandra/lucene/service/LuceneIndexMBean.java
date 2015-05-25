package com.stratio.cassandra.lucene.service;

import org.apache.lucene.document.Document;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public interface LuceneIndexMBean {

    /**
     * Commits the pending changes.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    void commit() throws IOException;

    /**
     * Returns the total number of {@link Document}s in this index.
     *
     * @return The total number of {@link Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    long getNumDocs() throws IOException;

    /**
     * Returns the total number of deleted {@link Document}s in this index.
     *
     * @return The total number of deleted {@link Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    long getNumDeletedDocs() throws IOException;

    /**
     * Optimizes the index forcing merge segments leaving the specified number of segments. This operation may block
     * until all merging completes.
     *
     * @param maxNumSegments The maximum number of segments left in the index after merging finishes.
     * @param doWait         {@code true} if the call should block until the operation completes.
     * @throws IOException If Lucene throws IO errors.
     */
    void forceMerge(int maxNumSegments, boolean doWait) throws IOException;

    /**
     * Optimizes the index forcing merge of all segments that have deleted documents.. This operation may block until
     * all merging completes.
     *
     * @param doWait {@code true} if the call should block until the operation completes.
     * @throws IOException If Lucene throws IO errors.
     */
    void forceMergeDeletes(boolean doWait) throws IOException;

    /**
     * Refreshes the index readers.
     */
    void refresh();
}
