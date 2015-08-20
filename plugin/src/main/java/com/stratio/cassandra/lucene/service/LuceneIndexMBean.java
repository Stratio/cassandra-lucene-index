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

import java.io.IOException;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public interface LuceneIndexMBean {

    /**
     * Commits the pending changes.
     *
     * @throws IOException If Lucene throws IO errors.
     */
    void commit() throws IOException;

    /**
     * Returns the total number of {@link org.apache.lucene.document.Document}s in this index.
     *
     * @return The total number of {@link org.apache.lucene.document.Document}s in this index.
     * @throws IOException If Lucene throws IO errors.
     */
    long getNumDocs() throws IOException;

    /**
     * Returns the total number of deleted {@link org.apache.lucene.document.Document}s in this index.
     *
     * @return The total number of deleted {@link org.apache.lucene.document.Document}s in this index.
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
     *
     * @throws IOException If Lucene throws IO errors.
     */
    void refresh() throws IOException;
}
