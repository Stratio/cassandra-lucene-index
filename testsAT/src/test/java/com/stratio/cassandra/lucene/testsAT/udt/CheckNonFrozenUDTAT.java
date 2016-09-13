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

/**
 * Now, cassandra (2.1.11, 2.2.3 ) demand that User Defined Types and collections Of UDF must be frozen but this may
 * change in next future so i include these tests to be able in next versions to detect this change because it is
 * problematic.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CheckNonFrozenUDTAT extends BaseAT {

    @Test
    public void testNotFrozenUDT() {
        CassandraUtils.builder("testNotFrozenUDT")
                      .withUDT("address_udt", "city", "text")
                      .withUDT("address_udt", "postcode", "int")
                      .withColumn("login", "text")
                      .withColumn("address", "address_udt")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createUDTs()
                      .createTable(InvalidQueryException.class,
                                   "Non-frozen User-Defined types are not supported, please use frozen<>")
                      .dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTList() {
        CassandraUtils.builder("testNotFrozenUDTList")
                      .withUDT("address_udt", "city", "text")
                      .withUDT("address_udt", "postcode", "int")
                      .withColumn("login", "text")
                      .withColumn("address", "list<address_udt>")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createUDTs()
                      .createTable(InvalidQueryException.class,
                                   "Non-frozen collections are not allowed inside collections: list<address_udt>")
                      .dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTSet() {
        CassandraUtils.builder("testNotFrozenUDTSet")
                      .withUDT("address_udt", "city", "text")
                      .withUDT("address_udt", "postcode", "int")
                      .withColumn("login", "text")
                      .withColumn("address", "set<address_udt>")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createUDTs()
                      .createTable(InvalidQueryException.class,
                                   "Non-frozen collections are not allowed inside collections: set<address_udt>")
                      .dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsKey() {
        CassandraUtils.builder("testNotFrozenUDTMapAsKey")
                      .withUDT("address_udt", "city", "text")
                      .withUDT("address_udt", "postcode", "int")
                      .withColumn("login", "text")
                      .withColumn("address", "map<address_udt, int>")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createUDTs()
                      .createTable(InvalidQueryException.class,
                                   "Non-frozen collections are not allowed inside collections: map<address_udt, int>")
                      .dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsValue() {
        CassandraUtils.builder("testNotFrozenUDTMapAsValue")
                      .withUDT("address_udt", "city", "text")
                      .withUDT("address_udt", "postcode", "int")
                      .withColumn("login", "text")
                      .withColumn("address", "map<int, address_udt>")
                      .withPartitionKey("login")
                      .build()
                      .createKeyspace()
                      .createUDTs()
                      .createTable(InvalidQueryException.class,
                                   "Non-frozen collections are not allowed inside collections: map<int, address_udt>")
                      .dropKeyspace();
    }
}
