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

import com.datastax.driver.core.SimpleStatement;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.UDT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@Ignore
@RunWith(JUnit4.class)
public class CollectionUpsertTest {

    /* This test checks if a not full insert is done what happens*/
    private static CassandraUtils cassandraUtils;
    static final String KEYSPACE_NAME = "collections_upsert";

    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " first_name text," +
                                    " second_name text," +
                                    " address_col list<frozen<address_udt>>, " +
                                    " frozen_address_col frozen<list<address_udt>>, " +
                                    " p_address frozen<address_udt>, " +
                                    "  lucene text);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        cassandraUtils.execute(new SimpleStatement(tableCreationQuery));

        String createIndexQuery = "CREATE CUSTOM INDEX test_index ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1'," +
                                  "'schema' : '{" +
                                  "fields : { " +
                                  "first_name : {type:\"string\"}," +
                                  "second_name : {type:\"string\"}," +
                                  "\"address_col.city\" : {type:\"string\"}," +
                                  "\"address_col.postcode\" : {type:\"integer\"}," +
                                  "\"p_address.city\" : {type:\"string\"}," +
                                  "\"p_address.postcode\" : {type:\"integer\"}" +
                                  "}" +
                                  "}'};";

        cassandraUtils.execute(new SimpleStatement(createIndexQuery));
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void testSimpleInsertThatNotRequiredRead() {
        /*not required read*/
        CassandraUtils cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();
        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " first_name text," +
                                    " second_name text);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(tableCreationQuery));

        String INSERT = "INSERT INTO " + cassandraUtils.getKeyspace() + "." + cassandraUtils.getTable() +
                        "( login,first_name,second_name)" +
                        "VALUES('login1','Peter','Handsome');";
        cassandraUtils.execute(new SimpleStatement(INSERT));

    }

    @Test
    public void testInsertWithAllRequiredData() {
        /*this nust not request a read */
        String INSERT = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "( login,first_name,second_name,address_col,p_address)" +
                        "VALUES('login1','Peter','Handsome',[{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}],{city:'Madrid',postcode:28945});";
        cassandraUtils.execute(new SimpleStatement(INSERT));

        //assert not read Generated

    }

    @Test
    public void testInsertWithNotAllMapperColumns() {
        /*this nust request a read */
        String INSERT = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "( login,second_name,address_col,p_address)" +
                        "VALUES('login2','Handsome',[{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}],{city:'Madrid',postcode:28945});";
        cassandraUtils.execute(new SimpleStatement(INSERT));

    }

    @Test
    public void testUpdateWithAllRequiredData() {
        String INSERT = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "( login,first_name,second_name,address_col,p_address)" +
                        "VALUES('login3','Peter','Handsome',[{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}],{city:'Madrid',postcode:28945});";

        cassandraUtils.execute(new SimpleStatement(INSERT));
        String UPDATE = "UPDATE " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        " SET first_name='David', second_name='Garcia',address_col=[{city:'Zaragoza',postcode:589654},{city:'Oviedo',postcode:54854}] ,p_address={city:'Zaragoza',postcode:589654}" +
                        " WHERE login='login3';";
        cassandraUtils.execute(new SimpleStatement(UPDATE));
        /*this nust request a read */
    }

    @Test
    public void testUpdateWithNotAllRequiredData() {
        /*this nust request a read */
        String INSERT = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "( login,first_name,second_name,address_col,p_address)" +
                        "VALUES('login4','Peter','Handsome',[{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}],{city:'Madrid',postcode:28945});";

        cassandraUtils.execute(new SimpleStatement(INSERT));
        String UPDATE = "UPDATE " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        " SET first_name='David', address_col=[{city:'Zaragoza',postcode:589654},{city:'Oviedo',postcode:54854}] ,p_address={city:'Zaragoza',postcode:589654}" +
                        " WHERE login='login4';";
        cassandraUtils.execute(new SimpleStatement(UPDATE));
    }

    @Test
    public void testUpdateWithPartialCollection() {
        /*this nust request a read */
         /*this nust request a read */
        String INSERT = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "( login,first_name,second_name,address_col,p_address)" +
                        "VALUES('login5','Peter','Handsome',[{city:'Madrid',postcode:28945},{city:'Berlin',postcode:25965}],{city:'Madrid',postcode:28945});";

        cassandraUtils.execute(new SimpleStatement(INSERT));
        String UPDATE = "UPDATE " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        " SET first_name='David',second_name='Garcia',address_col=address_col+[{city:'Zaragoza',postcode:589654},{city:'Oviedo',postcode:54854}] ,p_address={city:'Zaragoza',postcode:589654}" +
                        " WHERE login='login5';";
        cassandraUtils.execute(new SimpleStatement(UPDATE));
    }
}
