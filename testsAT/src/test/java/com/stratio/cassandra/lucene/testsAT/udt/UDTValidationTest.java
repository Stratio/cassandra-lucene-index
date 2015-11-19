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
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.UDT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTValidationTest {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder("udt_validation").build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";
        UDT geoPointUDT = new UDT("geo_point");
        geoPointUDT.add("latitude", "float");
        geoPointUDT.add("longitude", "float");

        UDT addressUDT = new UDT("address");
        addressUDT.add("street", "text");
        addressUDT.add("city", "text");
        addressUDT.add("zip", "int");
        addressUDT.add("bool", "boolean");
        addressUDT.add("hight", "float");
        addressUDT.add("point", "frozen<geo_point>");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY, first_name text, last_name text, addres frozen<address>, lucene text);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(geoPointUDT.build()));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        cassandraUtils.execute(new SimpleStatement(tableCreationQuery));
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void testValidCreateIndex() {

        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndexName() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"addres.city\" : {type:\"string\"}," +
                                  "\"addres.zip\" : {type:\"integer\"}," +
                                  "\"addres.bool\" : {type:\"boolean\"}, " +
                                  "\"addres.hight\" : {type:\"float\"}," +
                                  " first_name : {type:\"string\"}}}'};";

        ResultSet result = cassandraUtils.execute(new SimpleStatement(createIndexQuery));
        assertEquals("Creating valid udt index must return that was applied", true, result.wasApplied());

        String dropIndex = "DROP INDEX " + cassandraUtils.getIndexName() + ";";
        result = cassandraUtils.execute(new SimpleStatement(dropIndex));
        assertEquals("Dropping valid udt index must return that was applied", true, result.wasApplied());
    }

    @Test
    public void testInValidCreateIndex() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndexName() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  " \"addres.inexistent.latitude\" : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (InvalidConfigurationInQueryException e) {
            String
                    expectedMessage
                    = "'schema' is invalid : No column definition 'addres.inexistent' for mapper 'addres.inexistent.latitude'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex2() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndexName() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"addres.inexistent\" : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (InvalidConfigurationInQueryException e) {
            String
                    expectedMessage
                    = "'schema' is invalid : No column definition 'addres.inexistent' for mapper 'addres.inexistent'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex3() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndexName() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"addres.city\" : {type:\"string\"}," +
                                  "\"addres.zip\" : {type:\"integer\"}," +
                                  "\"addres.bool\" : {type:\"boolean\"}," +
                                  "\"addres.hight\" : {type:\"float\"}," +
                                  "\"addres.point.latitude\" : {type:\"float\"}," +
                                  "\"addres.point.longitude\" : {type:\"bytes\"}," +
                                  "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (InvalidConfigurationInQueryException e) {
            String
                    expectedMessage
                    = "'schema' is invalid : 'org.apache.cassandra.db.marshal.FloatType' is not supported by mapper 'addres.point.longitude'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex4() {
        String createIndexQuery = "CREATE CUSTOM INDEX " +
                                  cassandraUtils.getIndexName() +
                                  " ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1', " +
                                  "'schema' : '{ " +
                                  " fields : { " +
                                  "\"addres.city\" : {type:\"string\"}," +
                                  "\"addres.zip\" : {type:\"integer\"}," +
                                  "\"addres.bool\" : {type:\"boolean\"}," +
                                  "\"addres.hight\" : {type:\"float\"}," +
                                  "\"addres.point.latitude\" : {type:\"float\"}," +
                                  "\"addres.point.longitude.inexistent\" : {type:\"float\"}," +
                                  "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.execute(new SimpleStatement(createIndexQuery));
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (InvalidConfigurationInQueryException e) {
            String
                    expectedMessage
                    = "'schema' is invalid : No column definition 'addres.point.longitude.inexistent' for mapper 'addres.point.longitude.inexistent'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        }
    }

}
