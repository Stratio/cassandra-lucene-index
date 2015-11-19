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

package com.stratio.cassandra.lucene.testsAT.search;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.prefix;

@RunWith(JUnit4.class)
public class PrefixSearchAT extends AbstractSearchAT {

    @Test
    public void prefixQueryAsciiFieldTest1() {
        query(prefix("ascii_1", "frase ")).check(1);
    }

    @Test
    public void prefixQueryAsciiFieldTest2() {
        query(prefix("ascii_1", "frase")).check(4);
    }

    @Test
    public void prefixQueryAsciiFieldTest3() {
        query(prefix("ascii_1", "F")).check(0);
    }

    @Test
    public void prefixQueryAsciiFieldTest4() {
        query(prefix("ascii_1", "")).check(5);
    }

    @Test
    public void prefixQueryInetFieldTest1() {
        query(prefix("inet_1", "127")).check(4);
    }

    @Test
    public void prefixQueryInetFieldTest2() {
        query(prefix("inet_1", "")).check(5);
    }

    @Test
    public void prefixQueryInetFieldTest3() {
        query(prefix("inet_1", "127.0.")).check(2);
    }

    @Test
    public void prefixQueryTextFieldTest1() {
        query(prefix("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void prefixQueryTextFieldTest2() {
        query(prefix("text_1", "Frase")).check(0);
    }

    @Test
    public void prefixQueryTextFieldTest3() {
        query(prefix("text_1", "")).check(5);
    }

    @Test
    public void prefixQueryVarcharFieldTest1() {
        query(prefix("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void prefixQueryVarcharFieldTest2() {
        query(prefix("varchar_1", "frase")).check(4);
    }

    @Test
    public void prefixQueryVarcharFieldTest3() {
        query(prefix("varchar_1", "")).check(5);
    }

    @Test
    public void prefixQueryListFieldTest1() {
        query(prefix("list_1", "")).check(5);
    }

    @Test
    public void prefixQueryListFieldTest2() {
        query(prefix("list_1", "l1")).check(2);
    }

    @Test
    public void prefixQueryListFieldTest3() {
        query(prefix("list_1", "l")).check(5);
    }

    @Test
    public void prefixQueryListFieldTest4() {
        query(prefix("list_1", "s1")).check(0);
    }

    @Test
    public void prefixQuerySetFieldTest1() {
        query(prefix("set_1", "")).check(5);
    }

    @Test
    public void prefixQuerySetFieldTest2() {
        query(prefix("set_1", "l1")).check(0);
    }

    @Test
    public void prefixQuerySetFieldTest3() {
        query(prefix("set_1", "s1")).check(2);
    }

    @Test
    public void prefixQueryMapFieldTest1() {
        query(prefix("map_1$k1", "")).check(2);
    }

    @Test
    public void prefixQueryMapFieldTest2() {
        query(prefix("map_1$k1", "l1")).check(0);
    }

    @Test
    public void prefixQueryMapFieldTest3() {
        query(prefix("map_1$k1", "k1")).check(0);
    }

    @Test
    public void prefixQueryMapFieldTest4() {
        query(prefix("map_1$k1", "v1")).check(2);
    }

    @Test
    public void prefixFilterAsciiFieldTest1() {
        filter(prefix("ascii_1", "frase ")).check(1);
    }

    @Test
    public void prefixFilterAsciiFieldTest2() {
        filter(prefix("ascii_1", "frase")).check(4);
    }

    @Test
    public void prefixFilterAsciiFieldTest3() {
        filter(prefix("ascii_1", "F")).check(0);
    }

    @Test
    public void prefixFilterAsciiFieldTest4() {
        filter(prefix("ascii_1", "")).check(5);
    }

    @Test
    public void prefixFilterInetFieldTest1() {
        filter(prefix("inet_1", "127")).check(4);
    }

    @Test
    public void prefixFilterInetFieldTest2() {
        filter(prefix("inet_1", "")).check(5);
    }

    @Test
    public void prefixFilterInetFieldTest3() {
        filter(prefix("inet_1", "127.0.")).check(2);
    }

    @Test
    public void prefixFilterTextFieldTest1() {
        filter(prefix("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void prefixFilterTextFieldTest2() {
        filter(prefix("text_1", "Frase")).check(0);
    }

    @Test
    public void prefixFilterTextFieldTest3() {
        filter(prefix("text_1", "")).check(5);
    }

    @Test
    public void prefixFilterVarcharFieldTest1() {
        filter(prefix("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void prefixFilterVarcharFieldTest2() {
        filter(prefix("varchar_1", "frase")).check(4);
    }

    @Test
    public void prefixFilterVarcharFieldTest3() {
        filter(prefix("varchar_1", "")).check(5);
    }

    @Test
    public void prefixFilterListFieldTest1() {
        filter(prefix("list_1", "")).check(5);
    }

    @Test
    public void prefixFilterListFieldTest2() {
        filter(prefix("list_1", "l1")).check(2);
    }

    @Test
    public void prefixFilterListFieldTest3() {
        filter(prefix("list_1", "l")).check(5);
    }

    @Test
    public void prefixFilterListFieldTest4() {
        filter(prefix("list_1", "s1")).check(0);
    }

    @Test
    public void prefixFilterSetFieldTest1() {
        filter(prefix("set_1", "")).check(5);
    }

    @Test
    public void prefixFilterSetFieldTest2() {
        filter(prefix("set_1", "l1")).check(0);
    }

    @Test
    public void prefixFilterSetFieldTest3() {
        filter(prefix("set_1", "s1")).check(2);
    }

    @Test
    public void prefixFilterMapFieldTest1() {
        filter(prefix("map_1$k1", "")).check(2);
    }

    @Test
    public void prefixFilterMapFieldTest2() {
        filter(prefix("map_1$k1", "l1")).check(0);
    }

    @Test
    public void prefixFilterMapFieldTest3() {
        filter(prefix("map_1$k1", "k1")).check(0);
    }

    @Test
    public void prefixFilterMapFieldTest4() {
        filter(prefix("map_1$k1", "v1")).check(2);
    }
}
