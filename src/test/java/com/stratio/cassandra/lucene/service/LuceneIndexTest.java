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

import com.stratio.cassandra.lucene.IndexConfig;
import static junit.framework.Assert.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBuild() throws InterruptedException {
        Double refreshSeconds = 1.5;
        Path path = Paths.get(folder.newFolder("directory").getPath());
        LuceneIndex index = new LuceneIndex(path,
                                            refreshSeconds,
                                            IndexConfig.DEFAULT_RAM_BUFFER_MB,
                                            IndexConfig.DEFAULT_MAX_MERGE_MB,
                                            IndexConfig.DEFAULT_MAX_CACHED_MB,
                                            new StandardAnalyzer());
        Sort sort = new Sort(new SortField("field", SortField.Type.STRING));
        index.init(sort);
        assertEquals(0, index.getNumDocs());

        Term term1 = new Term("field1","value1");
        Document document1 = new Document();
        document1.add(new StringField("field1","value1", Field.Store.NO));
        index.upsert(term1, document1);

        Term term2 = new Term("field2","value2");
        Document document2 = new Document();
        document2.add(new StringField("field2","value2", Field.Store.NO));
        index.upsert(term2, document2);

        index.commit();
        Thread.sleep((int) (refreshSeconds * 1000));
        assertEquals(2, index.getNumDocs());

        index.delete(term1);
        Thread.sleep((int) (refreshSeconds * 1000));
        assertEquals(1, index.getNumDocs());

        index.close();
    }
}
