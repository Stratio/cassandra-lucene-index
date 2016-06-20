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

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTValidationAT extends BaseAT {

    private static CassandraUtilsBuilder cassandraUtilsBuilder;

    @BeforeClass
    public static void before() {
        cassandraUtilsBuilder = CassandraUtils.builder("udt_validation")
                                              .withTable("udt_validation_table")
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
                                              .withIndexColumn("lucene")
                                              .withPartitionKey("login");

    }

    @Test
    public void testValidCreateIndex() {
        cassandraUtilsBuilder.clone().withMapper("address.city", stringMapper())
                             .withMapper("address.zip", integerMapper())
                             .withMapper("address.bool", booleanMapper())
                             .withMapper("address.height", floatMapper())
                             .withMapper("first_name", stringMapper())
                             .build()
                             .createKeyspace()
                             .createUDTs()
                             .createTable().createIndex().dropIndex();
    }

    @Test
    public void testInvalidCreateIndex() {
        CassandraUtils cassandraUtils = cassandraUtilsBuilder.clone().withMapper("address.non-existent.latitude",
                                                                                 stringMapper())
                                                             .build()
                                                             .createKeyspace()
                                                             .createUDTs()
                                                             .createTable();

        try {
            cassandraUtils.createIndex();
            fail("Creating invalid index must throw an Exception but does not ");
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage = "'schema' is invalid : No column definition 'address.non-existent' " +
                                     "for mapper 'address.non-existent.latitude'";
            assertEquals(String.format(
                    "Creating invalid index must return InvalidConfigurationInQueryException(%s) " +
                    "but returns InvalidConfigurationInQueryException(%s)",
                    expectedMessage,
                    e.getMessage()), expectedMessage, e.getMessage());

        } finally {
            cassandraUtils.dropKeyspace();
        }
    }

    @Test
    public void testInvalidCreateIndex2() {

        CassandraUtils cassandraUtils = cassandraUtilsBuilder.clone().withMapper("address.non-existent",
                                                                                 stringMapper())
                                                             .build()
                                                             .createKeyspace()
                                                             .createUDTs()
                                                             .createTable();

        try {
            cassandraUtils.createIndex();
            fail("Creating invalid index must throw an Exception but does not ");
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage = "'schema' is invalid : No column definition 'address.non-existent' " +
                                     "for mapper 'address.non-existent'";
            assertEquals("Creating invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        } finally {
            cassandraUtils.dropKeyspace();
        }
    }

    @Test
    public void testInvalidCreateIndex3() {
        CassandraUtils cassandraUtils = cassandraUtilsBuilder.clone().withMapper("address.city", stringMapper())
                                                             .withMapper("address.zip", integerMapper())
                                                             .withMapper("address.bool", booleanMapper())
                                                             .withMapper("address.height", floatMapper())
                                                             .withMapper("address.point.latitude", floatMapper())
                                                             .withMapper("address.point.longitude", blobMapper())
                                                             .withMapper("first_name", stringMapper())
                                                             .build()
                                                             .createKeyspace()
                                                             .createUDTs()
                                                             .createTable();

        try {
            cassandraUtils.createIndex();
            fail("Creating invalid index must throw an Exception but does not ");
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage = "'schema' is invalid : Type 'org.apache.cassandra.db.marshal.FloatType' " +
                                     "in column 'address.point.longitude' " +
                                     "is not supported by mapper 'address.point.longitude'";
            assertEquals("Creating invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        } finally {
            cassandraUtils.dropKeyspace();
        }
    }

    @Test
    public void testInvalidCreateIndex4() {
        CassandraUtils cassandraUtils = cassandraUtilsBuilder.clone().withMapper("address.city", stringMapper())
                                                             .withMapper("address.zip", integerMapper())
                                                             .withMapper("address.bool", booleanMapper())
                                                             .withMapper("address.height", floatMapper())
                                                             .withMapper("address.point.latitude", floatMapper())
                                                             .withMapper("address.point.longitude.non-existent",
                                                                         floatMapper())
                                                             .withMapper("first_name", stringMapper())
                                                             .build()
                                                             .createKeyspace()
                                                             .createUDTs()
                                                             .createTable();

        try {
            cassandraUtils.createIndex();
            fail("Creating invalid index must throw an Exception but does not ");
        } catch (InvalidConfigurationInQueryException e) {
            String
                    expectedMessage
                    = "'schema' is invalid : No column definition 'address.point.longitude.non-existent' for mapper 'address.point.longitude.non-existent'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException(" +
                         expectedMessage +
                         ") but returns InvalidConfigurationInQueryException(" +
                         e.getMessage() +
                         ")", expectedMessage, e.getMessage());

        } finally {
            cassandraUtils.dropKeyspace();
        }
    }

}
