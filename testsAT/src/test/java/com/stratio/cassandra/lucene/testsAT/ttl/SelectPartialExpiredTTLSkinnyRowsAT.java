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
package com.stratio.cassandra.lucene.testsAT.ttl;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.stratio.cassandra.lucene.builder.Builder.match;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static org.junit.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SelectPartialExpiredTTLSkinnyRowsAT extends BaseAT {
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {

        utils = CassandraUtils.builder("test_ttl_parcial_skiny")
                              .withPartitionKey("a")
                              .withColumn("a", "int")
                              .withColumn("b", "text", stringMapper())
                              .withColumn("c", "text", stringMapper())
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
    }

    @Test
    public void testSkinyRowsParcialExpiredRows() throws InterruptedException {
        utils.insert(new String[]{"a", "b"}, new Object[]{1, "a"}, 5);
        utils.insert(new String[]{"a", "c"}, new Object[]{1, "b"});
        utils.insert(new String[]{"a", "c"}, new Object[]{2, "b"}, 10);
        utils.insert(new String[]{"a", "b"}, new Object[]{2, "a"});
        utils.insert(new String[]{"a", "b"}, new Object[]{3, "a"}, 12);
        utils.insert(new String[]{"a", "c"}, new Object[]{3, "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{4, "a", "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{5, "a", "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{6, "a", "c"});

        utils.flush();

        utils.insert(new String[]{"a", "b"}, new Object[]{11, "a"}, 5);
        utils.insert(new String[]{"a", "c"}, new Object[]{11, "b"});
        utils.insert(new String[]{"a", "b"}, new Object[]{12, "a"}, 10);
        utils.insert(new String[]{"a", "c"}, new Object[]{12, "b"});
        utils.insert(new String[]{"a", "b"}, new Object[]{13, "a"}, 12);
        utils.insert(new String[]{"a", "c"}, new Object[]{13, "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{14, "a", "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{15, "a", "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{16, "a", "c"});
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{17, "a", "c"});

        utils.flush();
        TimeUnit.SECONDS.sleep(15);
        utils.compact(false).refresh();
        utils.filter(match("b", "a")).checkUnorderedColumns("a", 2, 4, 5, 6, 14, 15, 16, 17);
        assertEquals("NumDocs in index is not correct", 13, utils.getIndexNumDocs());
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }
}
