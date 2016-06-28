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
package com.stratio.cassandra.lucene.testsAT.issues;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue94AT extends BaseAT {

    @Test
    public void testInsertExplicitlyNullColumns() {
        CassandraUtils utils = builder("issue_94").withTable("test")
                                                  .withIndexName("test")
                                                  .withPartitionKey("a")
                                                  .withColumn("a", "int", integerMapper())
                                                  .withColumn("b", "text", stringMapper())
                                                  .withColumn("c", "text", stringMapper())
                                                  .build()
                                                  .createKeyspace()
                                                  .createTable()
                                                  .createIndex();
        utils.insert(new String[]{"a", "b", "c"}, new Object[]{1, null, null})
             .execute("INSERT INTO %s(a , b , c ) VALUES (1, null, null);", utils.getQualifiedTable());
        utils.dropTable().dropKeyspace();
    }
}