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
package com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer;

import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenFilter.AsciifoldingTokenFilter;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenFilter.LowercaseTokenFilter;
import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenFilter.TokenFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static com.stratio.cassandra.lucene.builder.Builder.uuidMapper;
import static org.junit.Assert.assertEquals;

/**
 * Created by jpgilaberte on 2/06/17.
 */
public class CustomAnalyzertest {

    @Test
    public void testIndexFull() {
        String actual = index("ks", "table", "idx").keyspace("keyspace")
                .column("lucene")
                .directoryPath("path")
                .refreshSeconds(10D)
                .maxCachedMb(32)
                .maxMergeMb(16)
                .ramBufferMb(64)
                .indexingThreads(4)
                .indexingQueuesSize(100)
                .excludedDataCenters("DC1,DC2")
                .sparse(true)
                .partitioner(partitionerOnToken(8))
                .defaultAnalyzer("my_analyzer")
                .analyzer("my_analyzer", customAnalyzer(new WhitespaceTokenizer(),
                        null,
                        new TokenFilter[]{new AsciifoldingTokenFilter(), new LowercaseTokenFilter()}))
                .analyzer("snow", snowballAnalyzer("tartar").stopwords("a,b,c"))
                .mapper("uuid", uuidMapper().validated(true))
                .mapper("string", stringMapper())
                .build();
        String expected = "CREATE CUSTOM INDEX idx ON keyspace.table(lucene) " +
                "USING 'com.stratio.cassandra.lucene.Index' " +
                "WITH OPTIONS = {" +
                "'refresh_seconds':'10.0'," +
                "'directory_path':'path'," +
                "'ram_buffer_mb':'64'," +
                "'max_merge_mb':'16'," +
                "'max_cached_mb':'32'," +
                "'indexing_threads':'4'," +
                "'indexing_queues_size':'100'," +
                "'excluded_data_centers':'DC1,DC2'," +
                "'partitioner':'{\"type\":\"token\",\"partitions\":8}'," +
                "'sparse':'true'," +
                "'schema':'{" +
                "\"default_analyzer\":\"my_analyzer\",\"analyzers\":{" +
                "\"my_analyzer\":{\"type\":\"custom\",\"tokenizer\":{\"type\":\"whitespace\"},\"token_filter\":[{\"type\":\"asciifolding\",\"preserveOriginal\":false},{\"type\":\"lowercase\"}]}," +
                "\"snow\":{\"type\":\"snowball\",\"language\":\"tartar\",\"stopwords\":\"a,b,c\"}}," +
                "\"fields\":{" +
                "\"uuid\":{\"type\":\"uuid\",\"validated\":true},\"string\":{\"type\":\"string\"}}}'}";
        assertEquals("index serialization is wrong", expected, actual);
    }
}
