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
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.UDT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CheckNonFrozenUDTTest {
    /**
     * Now, cassandra (2.1.11, 2.2.3 ) demand that User Defined Types and collections Of
     * UDF must be frozen but this may change in next future so i include these tests to
     * be able in next versions to detect this change because it is problematic
     */
    private static CassandraUtils cassandraUtils;
    static final String KEYSPACE_NAME = "non_frozen_udt";

    @Test
    public void testNotFrozenUDT() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address address_udt);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        try {
            cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            String expectedMesssage = "Non-frozen User-Defined types are not supported, please use frozen<>";
            assertEquals("Getted exception with non expected message", expectedMesssage, e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTList() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address list<address_udt>);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        try {
            cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            String expectedMesssage = "Non-frozen collections are not allowed inside collections: list<address_udt>";
            assertEquals("Getted exception with non expected message", expectedMesssage, e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTSet() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address set<address_udt>);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        try {
            cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            String expectedMesssage = "Non-frozen collections are not allowed inside collections: set<address_udt>";
            assertEquals("Getted exception with non expected message", expectedMesssage, e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsKey() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address map<address_udt,int>);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        try {
            cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            String
                    expectedMesssage
                    = "Non-frozen collections are not allowed inside collections: map<address_udt, int>";
            assertEquals("Getted exception with non expected message", expectedMesssage, e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsValue() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address map<int,address_udt>);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        try {
            cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            String
                    expectedMesssage
                    = "Non-frozen collections are not allowed inside collections: map<int, address_udt>";
            assertEquals("Getted exception with non expected message", expectedMesssage, e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }
}
