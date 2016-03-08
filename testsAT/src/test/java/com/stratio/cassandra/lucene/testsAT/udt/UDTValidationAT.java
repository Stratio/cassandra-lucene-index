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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTValidationAT extends BaseAT {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {
        cassandraUtils = CassandraUtils.builder("udt_validation")
                                       .withUDT("geo_point", "latitude", "float")
                                       .withUDT("geo_point", "longitude", "float")
                                       .withUDT("address", "street", "text")
                                       .withUDT("address", "city", "text")
                                       .withUDT("address", "zip", "int")
                                       .withUDT("address", "bool", "boolean")
                                       .withUDT("address", "height", "float")
                                       .withUDT("address", "point", "frozen<geo_point>")
                                       .withColumn("login", "text")
                                       .withColumn("first_name", "text")
                                       .withColumn("last_name", "text")
                                       .withColumn("address", "frozen<address>")
                                       .withPartitionKey("login")
                                       .build()
                                       .createKeyspace()
                                       .createUDTs()
                                       .createTable();
    }

    @After
    public void after() {
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testValidCreateIndex() {

        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndex() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "() " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"address.city\" : {type:\"string\"}," +
                                  "\"address.zip\" : {type:\"integer\"}," +
                                  "\"address.bool\" : {type:\"boolean\"}, " +
                                  "\"address.height\" : {type:\"float\"}," +
                                  " first_name : {type:\"string\"}}}'};";

        ResultSet result = cassandraUtils.execute(new SimpleStatement(createIndexQuery));
        assertEquals("Creating valid udt index must return that was applied", true, result.wasApplied());
    }

    @Test
    public void testInValidCreateIndex() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndex() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "() " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  " \"address.non-existent.latitude\" : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (DriverException e) {
            String expectedMessage = "'schema' is invalid : No column definition 'address.non-existent' " +
                                     "for mapper 'address.non-existent.latitude'";
            assertEquals(String.format("Creating invalid index must return InvalidConfigurationInQueryException(%s) " +
                                       "but returns InvalidConfigurationInQueryException(%s)",
                                       expectedMessage,
                                       e.getMessage()), expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex2() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndex() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "() " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"address.non-existent\" : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (DriverException e) {
            String expectedMessage = "'schema' is invalid : No column definition 'address.non-existent' " +
                                     "for mapper 'address.non-existent'";
            assertEquals("Creating invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex3() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndex() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "() " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"address.city\" : {type:\"string\"}," +
                                  "\"address.zip\" : {type:\"integer\"}," +
                                  "\"address.bool\" : {type:\"boolean\"}," +
                                  "\"address.height\" : {type:\"float\"}," +
                                  "\"address.point.latitude\" : {type:\"float\"}," +
                                  "\"address.point.longitude\" : {type:\"bytes\"}," +
                                  "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (DriverException e) {
            String expectedMessage = "'schema' is invalid : 'org.apache.cassandra.db.marshal.FloatType'" +
                                     " is not supported by mapper 'address.point.longitude'";
            assertEquals("Creating invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex4() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndex() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "() " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"address.city\" : {type:\"string\"}," +
                                  "\"address.zip\" : {type:\"integer\"}," +
                                  "\"address.bool\" : {type:\"boolean\"}," +
                                  "\"address.height\" : {type:\"float\"}," +
                                  "\"address.point.latitude\" : {type:\"float\"}," +
                                  "\"address.point.longitude.non-existent\" : {type:\"float\"}," +
                                  "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (DriverException e) {
            String expectedMessage = "'schema' is invalid : No column definition " +
                                     "'address.point.longitude.non-existent' for mapper " +
                                     "'address.point.longitude.non-existent'";
            assertEquals("Creating invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

}
