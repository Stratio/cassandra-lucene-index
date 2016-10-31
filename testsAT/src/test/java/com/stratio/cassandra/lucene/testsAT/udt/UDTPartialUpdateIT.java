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

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTPartialUpdateIT extends BaseIT {
    private static Map<String, String> insertData1 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("id", "1");
                    put("address", "{ address:'fifth avenue', number:2}");
                }
            });

    private static Map<String, String> insertData2 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("id", "2");
                    put("address", "{ address:'eliot ave', number:45}");
                }
            });

    private static Map<String, String> insertData3 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("id", "3");
                    put("address", "{ address:'69th Ln', number:45}");
                }
            });

    private static Map<String, String> insertData4 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("id", "4");
                    put("address", "{ address:'HoneyWell st', number:105}");
                }
            });

    @Test
    public void testNonFrozenPartialUpdate() {
        CassandraUtils utils = builder("udt_partial_update")
                .withTable("partial_updates_table")
                .withUDT("address_t", "postal_code", "bigint")
                .withUDT("address_t", "number", "bigint")
                .withUDT("address_t", "address", "text")
                .withColumn("id", "bigint")
                .withColumn("address", "address_t")
                .withMapper("address.address", stringMapper())
                .withMapper("address.number", bigIntegerMapper())
                .withMapper("address.postal_code", bigIntegerMapper())
                .withIndexColumn("lucene")
                .withPartitionKey("id")
                .build()
                .createKeyspace()
                .createUDTs()
                .createTable()
                .createIndex()
                .insert(insertData1)
                .insert(insertData2)
                .insert(insertData3)
                .insert(insertData4);

        utils.filter(match("address.address", "fifth avenue")).refresh(true).checkUnorderedColumns("id", 1L)
             .filter(match("address.number", 2)).checkUnorderedColumns("id", 1L)
             .filter(match("address.postal_code", 10021)).check(0);

        utils.execute("UPDATE %s SET address.postal_code = 10021 WHERE id =1; ", utils.getQualifiedTable());

        utils.filter(match("address.address", "fifth avenue")).refresh(true).checkUnorderedColumns("id", 1L)
             .filter(match("address.number", 2)).checkUnorderedColumns("id", 1L)
             .filter(match("address.postal_code", 10021)).checkUnorderedColumns("id", 1L);

        utils.filter(match("address.address", "eliot ave")).refresh(true).checkUnorderedColumns("id", 2L)
             .filter(match("address.number", 45)).checkUnorderedColumns("id", 2L, 3L)
             .filter(match("address.postal_code", 50004)).check(0)
             .filter(match("address.address", "69th Ln")).refresh(true).checkUnorderedColumns("id", 3L)
             .filter(match("address.number", 45)).checkUnorderedColumns("id", 2L, 3L)
             .filter(match("address.postal_code", 558)).check(0)
             .filter(match("address.address", "HoneyWell st")).refresh(true).checkUnorderedColumns("id", 4L)
             .filter(match("address.number", 105)).checkUnorderedColumns("id", 4L)
             .filter(match("address.postal_code", 10020)).check(0);

        utils.dropTable().dropKeyspace();
    }
}
