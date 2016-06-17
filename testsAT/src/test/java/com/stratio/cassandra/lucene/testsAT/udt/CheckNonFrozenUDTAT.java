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
package com.stratio.cassandra.lucene.testsAT.udt;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Now, cassandra (2.1.11, 2.2.3 ) demand that User Defined Types and collections Of UDF must be frozen but this may
 * change in next future so i include these tests to be able in next versions to detect this change because it is
 * problematic.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CheckNonFrozenUDTAT extends BaseAT {

    private void test(CassandraUtils utils, String expectedMessage) {
        try {
            utils.createTable();
            assertTrue("This must return InvalidQueryException but does not", false);
        } catch (InvalidQueryException e) {
            assertEquals("Raised exception with non expected message", expectedMessage, e.getMessage());
        } finally {
            utils.dropKeyspace();
        }
    }

    @Test
    public void testNonFrozenUDTWithNestedNotFrozenCollections() {
        CassandraUtils utils = CassandraUtils.builder("nonFrozenUDTNested")
                                             .withUDT("address_udt", "city", "text")
                                             .withUDT("address_udt", "postcodes", "set<int>")
                                             .withColumn("login", "text")
                                             .withColumn("address", "address_udt")
                                             .withPartitionKey("login")
                                             .build()
                                             .createKeyspace()
                                             .createUDTs();
        test(utils, "Non-frozen UDTs with nested non-frozen collections are not supported");
    }

    @Test
    public void testNonFrozenUDTList() {
        CassandraUtils utils = CassandraUtils.builder("testNonFrozenUDTList")
                                             .withUDT("address_udt", "city", "text")
                                             .withUDT("address_udt", "postcode", "int")
                                             .withColumn("login", "text")
                                             .withColumn("address", "list<address_udt>")
                                             .withPartitionKey("login")
                                             .build()
                                             .createKeyspace()
                                             .createUDTs();
        test(utils, "Non-frozen UDTs are not allowed inside collections: list<address_udt>");
    }

    @Test
    public void testNonFrozenUDTSet() {
        CassandraUtils utils = CassandraUtils.builder("testNonFrozenUDTSet")
                                             .withUDT("address_udt", "city", "text")
                                             .withUDT("address_udt", "postcode", "int")
                                             .withColumn("login", "text")
                                             .withColumn("address", "set<address_udt>")
                                             .withPartitionKey("login")
                                             .build()
                                             .createKeyspace()
                                             .createUDTs();
        test(utils, "Non-frozen UDTs are not allowed inside collections: set<address_udt>");
    }

    @Test
    public void testNonFrozenUDTMapAsKey() {
        CassandraUtils utils = CassandraUtils.builder("testNonFrozenUDTMapAsKey")
                                             .withUDT("address_udt", "city", "text")
                                             .withUDT("address_udt", "postcode", "int")
                                             .withColumn("login", "text")
                                             .withColumn("address", "map<address_udt, int>")
                                             .withPartitionKey("login")
                                             .build()
                                             .createKeyspace()
                                             .createUDTs();
        test(utils, "Non-frozen UDTs are not allowed inside collections: map<address_udt, int>");
    }

    @Test
    public void testNonFrozenUDTMapAsValue() {
        CassandraUtils utils = CassandraUtils.builder("testNotFrozenUDTMapAsValue")
                                             .withUDT("address_udt", "city", "text")
                                             .withUDT("address_udt", "postcode", "int")
                                             .withColumn("login", "text")
                                             .withColumn("address", "map<int, address_udt>")
                                             .withPartitionKey("login")
                                             .build()
                                             .createKeyspace()
                                             .createUDTs();
        test(utils, "Non-frozen UDTs are not allowed inside collections: map<int, address_udt>");
    }
}
