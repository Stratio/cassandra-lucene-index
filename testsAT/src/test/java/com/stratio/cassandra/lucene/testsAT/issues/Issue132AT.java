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

import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test deletion of collection elements.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}.
 */
@RunWith(JUnit4.class)
public class Issue132AT extends BaseAT {

    @Test
    public void testUpdateSetRemovingUniqueComponentWide() {
        CassandraUtils utils = builder("issue_132_set_wide").withTable("test_wide_set")
                                                            .withIndexName("index_set_wide")
                                                            .withColumn("name", "text")
                                                            .withColumn("sec", "text")
                                                            .withColumn("myset", "set<text>", stringMapper())
                                                            .withPartitionKey("name")
                                                            .withClusteringKey("sec")
                                                            .build()
                                                            .createKeyspace()
                                                            .createTable()
                                                            .createIndex();
        String table = utils.getQualifiedTable();
        utils.execute("INSERT INTO %s (name,sec,myset) VALUES ('test1', 'continue', {'home'});", table);
        utils.refresh()
             .filter(wildcard("myset", "hom*")).check(1)
             .query(wildcard("myset", "hom*")).check(1)
             .execute("UPDATE %s SET myset = myset - {'home'} WHERE name='test1' AND sec = 'continue';", table);
        utils.refresh()
             .filter(wildcard("myset", "hom*")).check(0)
             .query(wildcard("myset", "hom*")).check(0)
             .dropKeyspace();
    }

    @Test
    public void testUpdateListRemovingUniqueComponentWide() {
        CassandraUtils utils = builder("issue_132_list_wide").withTable("test_wide_list")
                                                             .withIndexName("index_list_wide")
                                                             .withColumn("name", "text")
                                                             .withColumn("sec", "text")
                                                             .withColumn("mylist", "list<text>", stringMapper())
                                                             .withPartitionKey("name")
                                                             .withClusteringKey("sec")
                                                             .build()
                                                             .createKeyspace()
                                                             .createTable()
                                                             .createIndex();
        String table = utils.getQualifiedTable();

        utils.execute("INSERT INTO %s (name, sec,mylist) VALUES ('test1', 'continue', ['home']);", table);
        utils.refresh()
             .filter(wildcard("mylist", "hom*")).check(1)
             .query(wildcard("mylist", "hom*")).check(1)
             .execute("UPDATE %s SET mylist = mylist - ['home'] WHERE name='test1' AND sec = 'continue';", table);
        utils.refresh()
             .filter(wildcard("mylist", "hom*")).check(0)
             .query(wildcard("mylist", "hom*")).check(0)
             .dropKeyspace();
    }

    @Test
    public void testUpdateMapRemovingUniqueComponentWide() {
        CassandraUtils utils = builder("issue_132_map_wide").withTable("test_wide_map")
                                                            .withIndexName("index_map_wide")
                                                            .withPartitionKey("name")
                                                            .withClusteringKey("sec")
                                                            .withColumn("name", "text")
                                                            .withColumn("sec", "text")
                                                            .withColumn("mymap", "map<text,text>", stringMapper())
                                                            .build()
                                                            .createKeyspace()
                                                            .createTable()
                                                            .createIndex();
        String table = utils.getQualifiedTable();

        utils.execute("INSERT INTO %s (name, sec,mymap) VALUES ('test1','continue', {'home':'home'});", table);
        utils.refresh()
             .filter(wildcard("mymap$home", "hom*")).check(1)
             .query(wildcard("mymap$home", "hom*")).check(1)
             .execute("UPDATE %s SET mymap = mymap - {'home','home'} WHERE name='test1' AND sec = 'continue';", table);
        utils.refresh()
             .filter(wildcard("mymap$home", "hom*")).check(0)
             .query(wildcard("mymap$home", "hom*")).check(0)
             .dropKeyspace();
    }

    @Test
    public void testUpdateSetRemovingUniqueComponentSkinny() {
        CassandraUtils utils = builder("issue_132_set_skinny").withTable("test_skinny_set")
                                                              .withIndexName("index_set_skinny")
                                                              .withColumn("name", "text")
                                                              .withColumn("myset", "set<text>", stringMapper())
                                                              .withPartitionKey("name")
                                                              .build()
                                                              .createKeyspace()
                                                              .createTable()
                                                              .createIndex();
        String table = utils.getQualifiedTable();

        utils.execute("INSERT INTO %s (name, myset) VALUES ('test1', {'home'});", table);
        utils.refresh()
             .filter(wildcard("myset", "hom*")).check(1)
             .query(wildcard("myset", "hom*")).check(1)
             .execute("UPDATE %s SET myset = myset - {'home'} WHERE name='test1';", table);
        utils.refresh()
             .filter(wildcard("myset", "hom*")).check(0)
             .query(wildcard("myset", "hom*")).check(0)
             .dropKeyspace();
    }

    @Test
    public void testUpdateListRemovingUniqueComponentSkinny() {
        CassandraUtils utils = builder("issue_132_list_skinny").withTable("test_skinny_list")
                                                               .withIndexName("index_list_skinny")
                                                               .withPartitionKey("name")
                                                               .withColumn("name", "text")
                                                               .withColumn("mylist", "list<text>", stringMapper())
                                                               .build()
                                                               .createKeyspace()
                                                               .createTable()
                                                               .createIndex();
        String table = utils.getQualifiedTable();

        utils.execute("INSERT INTO %s (name, mylist) VALUES ('test1', ['home']);", table);
        utils.refresh()
             .filter(wildcard("mylist", "hom*")).check(1)
             .query(wildcard("mylist", "hom*")).check(1)
             .execute("UPDATE %s SET mylist = mylist - ['home'] WHERE name='test1';", table);
        utils.refresh()
             .filter(wildcard("mylist", "hom*")).check(0)
             .query(wildcard("mylist", "hom*")).check(0)
             .dropKeyspace();
    }

    @Test
    public void testUpdateMapRemovingUniqueComponentSkinny() {
        CassandraUtils utils = builder("issue_132_map_skinny").withTable("test_skinny_map")
                                                              .withIndexName("index_map_skinny")
                                                              .withPartitionKey("name")
                                                              .withColumn("name", "text")
                                                              .withColumn("mymap", "map<text,text>", stringMapper())
                                                              .build()
                                                              .createKeyspace()
                                                              .createTable()
                                                              .createIndex();
        String table = utils.getQualifiedTable();

        utils.execute("INSERT INTO %s (name, mymap) VALUES ('test1', {'home':'home'});", table);
        utils.refresh()
             .filter(wildcard("mymap$home", "hom*"))
             .check(1)
             .query(wildcard("mymap$home", "hom*"))
             .check(1)
             .execute("UPDATE %s SET mymap = mymap - {'home','home'} WHERE name='test1';", table);
        utils.refresh()
             .filter(wildcard("mymap", "hom*")).check(0)
             .query(wildcard("mymap", "hom*")).check(0)
             .dropKeyspace();
    }
}
