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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class DocumentIteratorTest {

    class AnalyzerMock extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            return null;
        }
    }

    class QueryMock extends Query {

        @Override
        public String toString(String s) {
            return null;
        }
    }

    class DirectoryMock extends Directory {

        @Override
        public String[] listAll() throws IOException {
            return new String[0];
        }

        @Override
        public void deleteFile(String s) throws IOException {

        }

        @Override
        public long fileLength(String s) throws IOException {
            return 0;
        }

        @Override
        public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
            return null;
        }

        @Override
        public void sync(Collection<String> collection) throws IOException {

        }

        @Override
        public void renameFile(String s, String s1) throws IOException {

        }

        @Override
        public IndexInput openInput(String s, IOContext ioContext) throws IOException {
            return null;
        }

        @Override
        public Lock obtainLock(String s) throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    class IndexWriterMock extends IndexWriter {

        public IndexWriterMock(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

    }

    @Test
    public void testConstructorWithPageEqualsZero() throws IOException {
        IndexWriterConfig iwConfig = new IndexWriterConfig(new AnalyzerMock());
        SearcherManager searcherManager = new SearcherManager(new IndexWriterMock(new DirectoryMock(), iwConfig),
                                                              true,
                                                              null);
        DocumentIterator docIterator = new DocumentIterator(searcherManager,
                                                            new Sort(), null, new QueryMock(),
                                                            new Sort(),
                                                            0,
                                                            new HashSet<>());

        assertEquals("document Iterator page is invalid", 1, docIterator.page);
    }

    @Test
    public void testConstructorWithPageEqualsOne() throws IOException {
        IndexWriterConfig iwConfig = new IndexWriterConfig(new AnalyzerMock());
        SearcherManager searcherManager = new SearcherManager(new IndexWriterMock(new DirectoryMock(), iwConfig),
                                                              true,
                                                              null);
        DocumentIterator docIterator = new DocumentIterator(searcherManager,
                                                            new Sort(), null, new QueryMock(),
                                                            new Sort(),
                                                            1,
                                                            new HashSet<>());

        assertEquals("document Iterator page is invalid", 2, docIterator.page);
    }

    @Test
    public void testConstructorWithPageEqualsMaxValue() throws IOException {
        IndexWriterConfig iwConfig = new IndexWriterConfig(new AnalyzerMock());
        SearcherManager searcherManager = new SearcherManager(new IndexWriterMock(new DirectoryMock(), iwConfig),
                                                              true,
                                                              null);
        DocumentIterator docIterator = new DocumentIterator(searcherManager,
                                                            new Sort(), null, new QueryMock(),
                                                            new Sort(),
                                                            DocumentIterator.MAX_PAGE_SIZE,
                                                            new HashSet<>());

        assertEquals("document Iterator page is invalid", DocumentIterator.MAX_PAGE_SIZE + 1, docIterator.page);
    }

    @Test
    public void testConstructorWithPageOverMaxValue() throws IOException {
        IndexWriterConfig iwConfig = new IndexWriterConfig(new AnalyzerMock());
        SearcherManager searcherManager = new SearcherManager(new IndexWriterMock(new DirectoryMock(), iwConfig),
                                                              true,
                                                              null);
        DocumentIterator docIterator = new DocumentIterator(searcherManager,
                                                            new Sort(), null, new QueryMock(),
                                                            new Sort(),
                                                            10000000,
                                                            new HashSet<>());

        assertEquals("document Iterator page is invalid", DocumentIterator.MAX_PAGE_SIZE + 1, docIterator.page);
    }
}
