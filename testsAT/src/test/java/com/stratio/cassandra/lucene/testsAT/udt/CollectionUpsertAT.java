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

package com.stratio.cassandra.lucene.testsAT.udt;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@Ignore
@RunWith(JUnit4.class)
public class CollectionUpsertAT extends BaseAT {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder("collections_upsert")
                                       .withUDT("address_udt", "city", "text")
                                       .withUDT("address_udt", "postcode", "int")
                                       .withColumn("login", "text")
                                       .withColumn("first_name", "text")
                                       .withColumn("second_name", "text")
                                       .withColumn("address_col", "list<frozen<address_udt>>")
                                       .withColumn("frozen_address_col", "frozen<list<address_udt>>")
                                       .withColumn("p_address", "frozen<address_udt>")
                                       .withPartitionKey("login")
                                       .withMapper("address_col.city", stringMapper())
                                       .withMapper("address_col.postcode", stringMapper())
                                       .withMapper("p_address.city", stringMapper())
                                       .withMapper("p_address.postcode", stringMapper())
                                       .build()
                                       .createKeyspace()
                                       .createUDTs()
                                       .createTable()
                                       .createIndex();
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void testSimpleInsertThatNotRequiresRead() {
        /* This must not request a read */
        CassandraUtils.builder("collections_upsert_2")
                      .withColumn("login", "text")
                      .withColumn("first_name", "text")
                      .withColumn("second_name", "text")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createTable()
                      .createIndex()
                      .insert(new String[]{"login", "first_name", "second_name"},
                              new Object[]{"login1", "Peter", "Handsome"});
    }

    @Test
    public void testInsertWithAllRequiredData() {
        /* This must not request a read */
        cassandraUtils.execute("INSERT INTO " +
                               cassandraUtils.getQualifiedTable() +
                               "(login, first_name, second_name, address_col, p_address)" +
                               "VALUES('login1','Peter','Handsome'," +
                               "[{city:'Madrid',postcode:28945}," +
                               " {city:'Berlin',postcode:25965}]," +
                               " {city:'Madrid',postcode:28945});");
    }

    @Test
    public void testInsertWithNotAllMapperColumns() {
        // This must request a read
        cassandraUtils.execute("INSERT INTO " +
                               cassandraUtils.getQualifiedTable() +
                               " (login, second_name, address_col, p_address)" +
                               " VALUES('login2', 'Handsome'," +
                               " [{city:'Madrid',postcode:28945}, {city:'Berlin',postcode:25965}]," +
                               " {city:'Madrid',postcode:28945});");
    }

    @Test
    public void testUpdateWithAllRequiredData() {
        // This must request a read
        cassandraUtils.execute("INSERT INTO " +
                               cassandraUtils.getQualifiedTable() +
                               " (login,first_name,second_name,address_col,p_address)" +
                               " VALUES('login3', 'Peter', 'Handsome'," +
                               " [{city:'Madrid',postcode:28945}," +
                               " {city:'Berlin',postcode:25965}]," +
                               " {city:'Madrid',postcode:28945});");
        ;
        cassandraUtils.execute("UPDATE " +
                               cassandraUtils.getQualifiedTable() +
                               " SET first_name='David', second_name='Garcia'," +
                               " address_col=[{city:'Zaragoza', postcode:589654}, {city:'Oviedo', postcode:54854}]," +
                               " p_address={city:'Zaragoza', postcode:589654}" +
                               " WHERE login='login3';");
    }

    @Test
    public void testUpdateWithNotAllRequiredData() {
        // This must request a read
        cassandraUtils.execute("INSERT INTO " +
                               cassandraUtils.getQualifiedTable() +
                               " (login,first_name,second_name,address_col,p_address)" +
                               " VALUES ('login4','Peter','Handsome'," +
                               " [{city:'Madrid',postcode:28945}," +
                               " {city:'Berlin',postcode:25965}]," +
                               " {city:'Madrid',postcode:28945});");
        cassandraUtils.execute("UPDATE " +
                               cassandraUtils.getQualifiedTable() +
                               " SET first_name='David', " +
                               "address_col=[{city:'Zaragoza',postcode:589654},{city:'Oviedo',postcode:54854}], " +
                               "p_address={city:'Zaragoza',postcode:589654} " +
                               "WHERE login='login4';");
    }

    @Test
    public void testUpdateWithPartialCollection() {
        // This must request a read
        cassandraUtils.execute("INSERT INTO " +
                               cassandraUtils.getQualifiedTable() +
                               "(login,first_name,second_name,address_col,p_address)" +
                               " VALUES ('login5', 'Peter', 'Handsome'," +
                               " [{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}]," +
                               " {city:'Madrid',postcode:28945});");
        cassandraUtils.execute("UPDATE " +
                               cassandraUtils.getQualifiedTable() +
                               " SET first_name='David', second_name='Garcia'," +
                               " address_col=address_col+" +
                               " [{city:'Zaragoza',postcode:589654},{city:'Oviedo',postcode:54854}]," +
                               " p_address={city:'Zaragoza',postcode:589654}" +
                               " WHERE login='login5';");
    }
}
