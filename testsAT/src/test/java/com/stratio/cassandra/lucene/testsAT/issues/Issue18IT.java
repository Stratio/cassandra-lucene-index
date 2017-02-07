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
package com.stratio.cassandra.lucene.testsAT.issues;

import com.google.common.collect.Maps;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.match;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue18IT extends BaseIT {

    @Test
    public void test() {
        Map<String, String> data1 = Maps.newHashMap();
        data1.put("idcol", "1");
        data1.put("testtextcol", "'row1'");
        data1.put("testmapcol", "{'attb1': 'row1attb1Val', 'attb2': 'row1attb2Val', 'attb3': 'row1attb3Val'}");

        Map<String, String> data2 = Maps.newHashMap();
        data2.put("idcol", "2");
        data2.put("testtextcol", "'someLongRow2Value'");
        data2.put("testmapcol", "{'attb1': 'someLongattb1Val', 'attb2': 'attb2Val', 'attb3': 'attb3Val'}");

        Map<String, String> data3 = Maps.newHashMap();
        data3.put("idcol", "3");
        data3.put("testtextcol", "'row3'");
        data3.put("testmapcol", "{'attb1': 'row2attb1Val', 'attb2': 'row2attb2Val', 'attb3': 'row2attb3Val'}");

        Map<String, String> data4 = Maps.newHashMap();
        data4.put("idcol", "4");
        data4.put("testtextcol", "'row4'");
        data4.put("testmapcol", "{'attb2': 'row3attb2Val', 'attb3': 'row3attb3Val'}");

        builder("issue_18")
                .withPartitionKey("idcol")
                .withColumn("idcol", "int")
                .withColumn("testtextcol", "text", stringMapper())
                .withColumn("testmapcol", "map<text,text>", stringMapper())
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(data1, data2, data3, data4)
                .refresh()
                .filter(match("testmapcol$attb1", "row1attb1Val"))
                .checkUnorderedColumns("idcol", 1)
                .dropTable()
                .dropKeyspace();
    }
}
