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

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LargeFieldIT extends BaseIT {

    @Test
    public void testLargeField() throws IOException {
        CassandraUtils utils = CassandraUtils.builder("large_field")
                                             .withPartitionKey("k")
                                             .withColumn("k", "int", null)
                                             .withColumn("t", "text", textMapper().analyzer("whitespace"))
                                             .withColumn("s", "text", stringMapper())
                                             .build()
                                             .createKeyspace()
                                             .createTable()
                                             .createIndex();

        int numNumbers = 5000;
        UUID[] numbers = new UUID[numNumbers];
        for (int i = 0; i < numNumbers; i++) {
            numbers[i] = UUID.randomUUID();
        }
        String largeString = Arrays.toString(numbers);

        utils.insert(new String[]{"k", "t", "s"}, new Object[]{1, "a", "b"})
             .insert(new String[]{"k", "t", "s"}, new Object[]{2, largeString, "b"})
             .insert(new String[]{"k", "t", "s"}, new Object[]{3, "a", largeString})
             .refresh()
             .searchAll().check(3)
             .filter(wildcard("t", "*")).check(3)
             .filter(wildcard("s", "*")).check(2)
             .dropKeyspace();
    }
}
