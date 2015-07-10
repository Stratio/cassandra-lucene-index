/*
 * Copyright 2015, Stratio.
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

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.IndexConfig;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneIndexTest {

    private static final Double REFRESH_SECONDS = 0.1D;
    private static final int REFRESH_MILLISECONDS = (int) (REFRESH_SECONDS * 1000);
    private static final int WAIT_MILLISECONDS = REFRESH_MILLISECONDS * 2;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCRUD() throws IOException, InterruptedException {

        Path path = Paths.get(folder.newFolder("directory" + UUID.randomUUID()).getPath());
        LuceneIndex index = new LuceneIndex("ks",
                                            "cf",
                                            "idx",
                                            path,
                                            IndexConfig.DEFAULT_RAM_BUFFER_MB,
                                            IndexConfig.DEFAULT_MAX_MERGE_MB,
                                            IndexConfig.DEFAULT_MAX_CACHED_MB,
                                            new StandardAnalyzer(),
                                            REFRESH_SECONDS,
                                            null);
        Sort sort = new Sort(new SortField("field", SortField.Type.STRING));
        index.init(sort);
        assertEquals(0, index.getNumDocs());

        Term term1 = new Term("field", "value1");
        Document document1 = new Document();
        document1.add(new StringField("field", "value1", Field.Store.NO));
        document1.add(new SortedDocValuesField("field", new BytesRef("value1")));
        index.upsert(term1, document1);

        Term term2 = new Term("field", "value2");
        Document document2 = new Document();
        document2.add(new StringField("field", "value2", Field.Store.NO));
        document2.add(new SortedDocValuesField("field", new BytesRef("value2")));
        index.upsert(term2, document2);

        index.commit();
        Thread.sleep(REFRESH_MILLISECONDS);
        assertEquals(2, index.getNumDocs());

        Query query = new WildcardQuery(new Term("field", "value*"));
        Set<String> fields = Sets.newHashSet("field");
        Map<Document, ScoreDoc> results;

        // Search
        SearcherManager searcherManager = index.getSearcherManager();
        IndexSearcher searcher = searcherManager.acquire();

        try {
            results = index.search(searcher, query, null, null, 1, fields, true);
            assertEquals(1, results.size());
            ScoreDoc last1 = results.values().iterator().next();
            results = index.search(searcher, query, null, last1, 1, fields, true);
            assertEquals(1, results.size());

            results = index.search(searcher, query, null, null, 1, fields, false);
            assertEquals(1, results.size());
            ScoreDoc last2 = results.values().iterator().next();
            results = index.search(searcher, query, null, last2, 1, fields, false);
            assertEquals(1, results.size());

            results = index.search(searcher, query, sort, null, 1, fields, false);
            assertEquals(1, results.size());
            ScoreDoc last3 = results.values().iterator().next();
            results = index.search(searcher, query, sort, last3, 1, fields, false);
            assertEquals(1, results.size());
        } finally {
            searcherManager.release(searcher);
        }

        // Delete by term
        index.delete(term1);
        index.commit();
        Thread.sleep(WAIT_MILLISECONDS);
        assertEquals(1, index.getNumDocs());

        // Delete by query
        index.upsert(term1, document1);
        index.commit();
        Thread.sleep(WAIT_MILLISECONDS);
        assertEquals(2, index.getNumDocs());
        index.delete(new TermQuery(term1));
        Thread.sleep(WAIT_MILLISECONDS);
        assertEquals(1, index.getNumDocs());

        // Upsert
        index.upsert(term1, document1);
        index.upsert(term2, document2);
        index.upsert(term2, document2);
        index.commit();
        Thread.sleep(WAIT_MILLISECONDS);
        assertEquals(2, index.getNumDocs());

        // Truncate
        index.truncate();
        index.commit();
        Thread.sleep(WAIT_MILLISECONDS);
        assertEquals(0, index.getNumDocs());

        // Delete
        index.delete();

        // Cleanup
        folder.delete();
    }
}
