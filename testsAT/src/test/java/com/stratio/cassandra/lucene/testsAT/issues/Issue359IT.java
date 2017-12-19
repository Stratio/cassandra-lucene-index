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

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.builder.Builder.match;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class Issue359IT extends BaseIT {
    protected static final Map<String, String> data1, data2, data3, data4;
    static {
        data1 = new LinkedHashMap<>();
        data1.put("login", "'jsmith'");
        data1.put("telephone", "'5'");
        data1.put("first_name", "'John'");
        data1.put("last_name", "'Smith'");

        data2 = new LinkedHashMap<>();
        data2.put("login", "'pocahontas'");
        data2.put("telephone", "'5'");
        data2.put("first_name", "'Poca'");
        data2.put("last_name", "'hontas'");

        data3 = new LinkedHashMap<>();
        data3.put("login", "'jsmith'");
        data3.put("telephone", "'15'");
        data3.put("first_name", "'John'");
        data3.put("last_name", "'Smith'");

        data4 = new LinkedHashMap<>();
        data4.put("login", "'pocahontas'");
        data4.put("telephone", "'15'");
        data4.put("first_name", "'Poca'");
        data4.put("last_name", "'hontas'");
    }

    @Test
    public void testWide() {
        builder("issue_359").withTable("frozen_list_as_clustering")
                              .withIndexName("frozen_list_as_clustering_index")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("telephone", "text", longMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("telephone")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data1, data2, data3, data4)
                              .filter(must(match("telephone", "5"))).refresh(true).check(2)
                              .filter(must(match("telephone", "15"))).refresh(true).check(2)
                              .checkNumDocsInIndex(4)
                              .delete().where("login", "jsmith").and("telephone","5")
                              .filter(all()).refresh(true).check(3)
                              .checkNumDocsInIndex(3)
                              .dropKeyspace();
    }
}