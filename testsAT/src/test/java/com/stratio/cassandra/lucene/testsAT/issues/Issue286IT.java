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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test indexing collections being part of clustering key (<a href="https://github.com/Stratio/cassandra-lucene-index/issues/286">issue 286</a>).
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue286IT extends BaseIT {

    protected static final Map<String, String> data1, data2, data3, data4, data5, data6, data7, data8;
    private static List<CassandraUtils> keyspacesToDrop = new ArrayList<>();

    static {
        data1 = new LinkedHashMap<>();
        data1.put("login", "'jsmith'");
        data1.put("first_name", "'John'");
        data1.put("last_name", "'Smith'");
        data1.put("telephones", "['5','7','25']");

        data2 = new LinkedHashMap<>();
        data2.put("login", "'pocahontas'");
        data2.put("first_name", "'Poca'");
        data2.put("last_name", "'hontas'");
        data2.put("telephones", "['29','346','853']");

        data3 = new LinkedHashMap<>();
        data3.put("login", "'jsmith'");
        data3.put("first_name", "'John'");
        data3.put("last_name", "'Smith'");
        data3.put("telephones", "{'5','7','25'}");

        data4 = new LinkedHashMap<>();
        data4.put("login", "'pocahontas'");
        data4.put("first_name", "'Poca'");
        data4.put("last_name", "'hontas'");
        data4.put("telephones", "{'29','346','853'}");

        data5 = new LinkedHashMap<>();
        data5.put("login", "'jsmith'");
        data5.put("first_name", "'John'");
        data5.put("last_name", "'Smith'");
        data5.put("addresses", "{'London': 'Camden Road', 'Madrid': 'Buenavista'}");

        data6 = new LinkedHashMap<>();
        data6.put("login", "'jsmith'");
        data6.put("first_name", "'John'");
        data6.put("last_name", "'Smith'");
        data6.put("addresses", "('London', 'Camden Road')");

        data7 = new LinkedHashMap<>();
        data7.put("login", "'pocahontas'");
        data7.put("first_name", "'Poca'");
        data7.put("last_name", "'hontas'");
        data7.put("addresses", "('Madrid', 'Buenavista')");

        data8 = new LinkedHashMap<>();
        data8.put("login", "'jsmith'");
        data8.put("first_name", "'John'");
        data8.put("last_name", "'Smith'");
        data8.put("address", "{street: 'Camden Road', city: 'London', zip: 'NW1 9NF'}");
    }

    @AfterClass
    public static void afterClass() {
        keyspacesToDrop.forEach(CassandraUtils::dropKeyspaceIfNotNull);
    }

    @Test
    public void testFrozenListAsClusteringKey() {
        builder("issue_286_1").withTable("frozen_list_as_clustering")
                              .withIndexName("frozen_list_as_clustering_index")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("telephones", "frozen<list<text>>", longMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("telephones")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data1, data2)
                              .filter(must(match("telephones", "853"), match("login", "pocahontas"))).refresh(true)
                              .checkUnorderedColumns("login", "pocahontas")
                              .dropKeyspace();
    }

    @Test
    public void testFrozenSetAsClusteringKey() {
        builder("issue_286_2").withTable("frozen_set_as_clustering")
                              .withIndexName("frozen_set_as_clustering_index")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("telephones", "frozen<set<text>>", longMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("telephones")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data3, data4)
                              .filter(must(match("telephones", 25L), match("login", "jsmith"))).refresh(true)
                              .checkUnorderedColumns("login", "jsmith")
                              .dropKeyspace();
    }

    @Test
    public void testFrozenMapAsClusteringKey() {
        builder("issue_286_3").withTable("frozen_map_as_clustering")
                              .withIndexName("frozen_map_as_clustering_index")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("addresses", "frozen<map<text,text>>", stringMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("addresses")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data5)
                              .filter(must(match("addresses$London", "Camden Road"), match("login", "jsmith"))).refresh(true)
                              .check(1)
                              .dropKeyspace();
    }

    //tuples are frozen bu default, you cannot do partial writes
    @Test
    public void testFrozenTupleAsClusteringKey() {
        builder("issue_286_4").withTable("frozen_tuple_as_clustering")
                              .withIndexName("frozen_tuple_as_clustering_index")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("addresses", "tuple<text,text>", null)
                              .withMapper("addresses.0", stringMapper())
                              .withMapper("addresses.1", stringMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("addresses")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data6, data7)
                              .filter(must(match("addresses.0", "London"), match("login", "jsmith"))).refresh(true)
                              .checkUnorderedColumns("login", "jsmith")
                              .filter(must(match("addresses.1", "Buenavista"), match("login", "pocahontas")))
                              .checkUnorderedColumns("login", "pocahontas")
                              .dropKeyspace();
    }

    @Test
    public void testFrozenUdtAsClusteringKey() {
        builder("issue_286_5").withTable("frozen_tuple_as_clustering")
                              .withIndexName("frozen_tuple_as_clustering_index")
                              .withUDT("address_t", "street", "text")
                              .withUDT("address_t", "city", "text")
                              .withUDT("address_t", "zip", "text")
                              .withUDT("address_t", "height", "float")
                              .withColumn("login", "text", stringMapper())
                              .withColumn("first_name", "text", stringMapper())
                              .withColumn("last_name", "text", stringMapper())
                              .withColumn("address", "frozen<address_t>")
                              .withMapper("address.street", stringMapper())
                              .withMapper("address.city", stringMapper())
                              .withMapper("address.zip", stringMapper())
                              .withPartitionKey("login")
                              .withClusteringKey("address")
                              .build()
                              .createKeyspace()
                              .createUDTs()
                              .createTable()
                              .createIndex()
                              .insert(data8)
                              .filter(must(match("address.zip", "NW1 9NF"), match("login", "jsmith"))).refresh(true)
                              .check(1)
                              .dropKeyspace();
    }

    @Test
    public void testIFCassandraSupportNonFrozenListAsClusteringKey() {
        CassandraUtils utils = builder("issue_286_6").withTable("frozen_map_as_clustering")
                                                     .withIndexName("frozen_map_as_clustering_index")
                                                     .withColumn("login", "text", stringMapper())
                                                     .withColumn("first_name", "text", stringMapper())
                                                     .withColumn("last_name", "text", stringMapper())
                                                     .withColumn("telephones", "list<bigint>", longMapper())
                                                     .withPartitionKey("login")
                                                     .withClusteringKey("telephones")
                                                     .build()
                                                     .createKeyspace();
        keyspacesToDrop.add(utils);
        utils.createTable(InvalidQueryException.class,
                          "Invalid non-frozen collection type for PRIMARY KEY component telephones");
    }

    @Test
    public void testIFCassandraSupportNonFrozenSetAsClusteringKey() {
        CassandraUtils utils = builder("issue_286_7").withTable("frozen_map_as_clustering")
                                                     .withIndexName("frozen_map_as_clustering_index")
                                                     .withColumn("login", "text", stringMapper())
                                                     .withColumn("first_name", "text", stringMapper())
                                                     .withColumn("last_name", "text", stringMapper())
                                                     .withColumn("telephones", "set<bigint>", longMapper())
                                                     .withPartitionKey("login")
                                                     .withClusteringKey("telephones")
                                                     .build()
                                                     .createKeyspace();
        keyspacesToDrop.add(utils);
        utils.createTable(InvalidQueryException.class,
                          "Invalid non-frozen collection type for PRIMARY KEY component telephones");
    }

    @Test
    public void testIFCassandraSupportNonFrozenMapAsClusteringKey() {
        CassandraUtils utils = builder("issue_286_8").withTable("frozen_map_as_clustering")
                                                     .withIndexName("frozen_map_as_clustering_index")
                                                     .withColumn("login", "text", stringMapper())
                                                     .withColumn("first_name", "text", stringMapper())
                                                     .withColumn("last_name", "text", stringMapper())
                                                     .withColumn("telephones", "map<string,bigint>", null)
                                                     .withPartitionKey("login")
                                                     .withClusteringKey("telephones")
                                                     .build()
                                                     .createKeyspace();
        keyspacesToDrop.add(utils);
        utils.createTable(InvalidQueryException.class,
                          "Non-frozen UDTs are not allowed inside collections: map<string, bigint>");
    }

    @Test
    public void testIFCassandraSupportNonFrozenUdtAsClusteringKey() {
        CassandraUtils utils = builder("issue_286_10").withTable("frozen_tuple_as_clustering")
                                                      .withIndexName("frozen_tuple_as_clustering_index")
                                                      .withUDT("address_t", "street", "text")
                                                      .withUDT("address_t", "city", "text")
                                                      .withUDT("address_t", "zip", "text")
                                                      .withUDT("address_t", "height", "float")
                                                      .withColumn("login", "text")
                                                      .withColumn("first_name", "text")
                                                      .withColumn("last_name", "text")
                                                      .withColumn("address", "address_t")
                                                      .withPartitionKey("login")
                                                      .withClusteringKey("address")
                                                      .build()
                                                      .createKeyspace()
                                                      .createUDTs();
        keyspacesToDrop.add(utils);
        utils.createTable(InvalidQueryException.class,
                          "Invalid non-frozen user-defined type for PRIMARY KEY component address");
    }
}
