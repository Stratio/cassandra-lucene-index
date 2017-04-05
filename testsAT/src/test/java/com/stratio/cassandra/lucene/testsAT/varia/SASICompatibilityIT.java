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
package com.stratio.cassandra.lucene.testsAT.varia;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.datastax.driver.core.querybuilder.QueryBuilder.like;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertEquals;

/**
 * Test compatibility between Lucene and SASI indexes.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class SASICompatibilityIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("sasi_compatibility")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int")
                              .withColumn("rc", "text", textMapper().analyzer("en"))
                              .withAnalyzer("en", snowballAnalyzer("English"))
                              .build()
                              .createKeyspace()
                              .createTable();
        utils.insert("pk,rc", 1, "the dogs and the cats");
    }

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testSearchWithLucene() {
        utils.createIndex().refresh()
             .filter(match("rc", "the")).check(0)
             .filter(match("rc", "and")).check(0)
             .filter(match("rc", "dog")).check(1)
             .filter(match("rc", "dogs")).check(1)
             .filter(match("rc", "cat")).check(1)
             .filter(match("rc", "cats")).check(1)
             .filter(phrase("rc", "the dogs")).check(1)
             .filter(phrase("rc", "the cats")).check(1)
             .filter(phrase("rc", "the dogs and the cats")).check(1);
    }

    @Test
    public void testSearchWithSASI() throws InterruptedException {
        String ks = utils.getKeyspace();
        String cf = utils.getTable();
        try {
            utils.execute("CREATE CUSTOM INDEX IF NOT EXISTS sasi_idx ON %s.%s(rc)" +
                          "USING 'org.apache.cassandra.index.sasi.SASIIndex'" +
                          "WITH OPTIONS = { 'mode': 'CONTAINS'," +
                          "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                          "'analyzed': 'true'," +
                          "'tokenization_enable_stemming': 'true'," +
                          "'tokenization_locale': 'en' };", ks, cf);
        } catch (InvalidConfigurationInQueryException e) {
            if (e.getMessage()
                 .equals("Unable to find custom indexer class 'org.apache.cassandra.index.sasi.SASIIndex'")) {
                logger.info("Ignoring SASI index test because they are not supported by current Cassandra version");
                return;
            }
        }
        Thread.sleep(2000); // Wait for index creation
        assertEquals("Expected 1 row", 1, utils.execute(select().from(ks, cf).where(like("rc", "dog"))).all().size());
        assertEquals("Expected 1 row", 1, utils.execute(select().from(ks, cf).where(like("rc", "dogs"))).all().size());
        assertEquals("Expected 1 row", 1, utils.execute(select().from(ks, cf).where(like("rc", "cat"))).all().size());
        assertEquals("Expected 1 row", 1, utils.execute(select().from(ks, cf).where(like("rc", "cats"))).all().size());
    }
}