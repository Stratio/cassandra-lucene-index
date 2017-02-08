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

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class UDFsIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("udfs")
                              .withPartitionKey("key")
                              .withColumn("key", "uuid")
                              .withColumn("value", "int")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 1})
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 2})
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 3})
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 4})
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 5})
                              .insert(new String[]{"key", "value"}, new Object[]{UUID.randomUUID(), 6})
                              .refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    @Test
    public void testUDF() {
        utils.execute("CREATE OR REPLACE FUNCTION %s.double (input int)\n" +
                      "RETURNS NULL ON NULL INPUT\n" +
                      "RETURNS bigint\n" +
                      "LANGUAGE java AS\n" +
                      "'return input * 2L;';", utils.getKeyspace());

        List<Row> rows = utils.refresh()
                              .execute(2, "SELECT key, double(value) FROM %s WHERE expr(%s,'{}');",
                                       utils.getQualifiedTable(), utils.getIndexName()).all();

        long[] expected = new long[]{2, 4, 6, 8, 10, 12};
        long[] actual = new long[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            actual[i] = rows.get(i).getLong(1);
        }
        Arrays.sort(expected);
        Arrays.sort(actual);
        String msg = String.format("Expected %s but found %s", Arrays.toString(expected), Arrays.toString(actual));
        assertArrayEquals(msg, expected, actual);
    }

    @Test
    public void testUDAF() {
        utils.execute("CREATE FUNCTION %s.averageState ( state tuple<int,bigint>, val int )\n" +
                      "  CALLED ON NULL INPUT\n" +
                      "  RETURNS tuple<int,bigint>\n" +
                      "  LANGUAGE java\n" +
                      "  AS '\n" +
                      "    if (val != null) {\n" +
                      "      state.setInt(0, state.getInt(0)+1);\n" +
                      "      state.setLong(1, state.getLong(1)+val.intValue());\n" +
                      "    }\n" +
                      "    return state;\n" +
                      "  ';", utils.getKeyspace());

        utils.execute("CREATE FUNCTION %s.averageFinal ( state tuple<int,bigint> )\n" +
                      "  CALLED ON NULL INPUT\n" +
                      "  RETURNS double\n" +
                      "  LANGUAGE java\n" +
                      "  AS '\n" +
                      "    double r = 0;\n" +
                      "    if (state.getInt(0) == 0) return null;\n" +
                      "    r = state.getLong(1);\n" +
                      "    r /= state.getInt(0);\n" +
                      "    return Double.valueOf(r);\n" +
                      "  ';", utils.getKeyspace());

        utils.execute("CREATE AGGREGATE %s.average ( int )\n" +
                      "  SFUNC averageState\n" +
                      "  STYPE tuple<int,bigint>\n" +
                      "  FINALFUNC averageFinal\n" +
                      "  INITCOND (0, 0);", utils.getKeyspace());

        List<Row> rows = utils.refresh()
                              .execute(2, "SELECT average(value) FROM %s WHERE expr(%s,'{}');",
                                       utils.getQualifiedTable(), utils.getIndexName()).all();

        assertEquals("Expected one row!", 1, rows.size());
        assertEquals("Expected 4!", 3.5D, rows.get(0).getDouble(0), 0);
    }
}
