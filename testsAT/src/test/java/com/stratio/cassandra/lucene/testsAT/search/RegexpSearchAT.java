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

import static com.stratio.cassandra.lucene.builder.Builder.regexp;

@RunWith(JUnit4.class)
public class RegexpSearchAT extends AbstractSearchAT {

    @Test
    public void regexpQueryAsciiFieldTest1() {
        query(regexp("ascii_1", "frase.*")).check(4);
    }

    @Test
    public void regexpQueryAsciiFieldTest2() {
        query(regexp("ascii_1", "frase .*")).check(1);
    }

    @Test
    public void regexpQueryAsciiFieldTest3() {
        query(regexp("ascii_1", ".*")).check(5);
    }

    @Test
    public void regexpQueryAsciiFieldTest4() {
        query(regexp("ascii_1", "")).check(0);
    }

    @Test
    public void regexpQueryAsciiFieldTest5() {
        query(regexp("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void regexpQueryInetFieldTest1() {
        query(regexp("inet_1", ".*")).check(5);
    }

    @Test
    public void regexpQueryInetFieldTest2() {
        query(regexp("inet_1", "127.*")).check(4);
    }

    @Test
    public void regexpQueryInetFieldTest3() {
        query(regexp("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void regexpQueryInetFieldTest4() {
        query(regexp("inet_1", "")).check(0);
    }

    @Test
    public void regexpQueryInetFieldTest5() {
        query(regexp("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void regexpQueryTextFieldTest1() {
        query(regexp("text_1", ".*")).check(5);
    }

    @Test
    public void regexpQueryTextFieldTest2() {
        query(regexp("text_1", "frase.*")).check(4);
    }

    @Test
    public void regexpQueryTextFieldTest3() {
        query(regexp("text_1", "frase .*")).check(0);
    }

    @Test
    public void regexpQueryTextFieldTest4() {
        query(regexp("text_1", "")).check(0);
    }

    @Test
    public void regexpQueryTextFieldTest5() {
        query(regexp("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void regexpQueryVarcharFieldTest1() {
        query(regexp("varchar_1", ".*")).check(5);
    }

    @Test
    public void regexpQueryVarcharFieldTest2() {
        query(regexp("varchar_1", "frase.*")).check(4);
    }

    @Test
    public void regexpQueryVarcharFieldTest3() {
        query(regexp("varchar_1", "frase .*")).check(1);
    }

    @Test
    public void regexpQueryVarcharFieldTest4() {
        query(regexp("varchar_1", "")).check(0);
    }

    @Test
    public void regexpQueryVarcharFieldTest5() {
        query(regexp("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void regexpQueryListFieldTest1() {
        query(regexp("list_1", "")).check(0);
    }

    @Test
    public void regexpQueryListFieldTest2() {
        query(regexp("list_1", "l.*")).check(5);
    }

    @Test
    public void regexpQueryListFieldTest3() {
        query(regexp("list_1", "s.*")).check(0);
    }

    @Test
    public void regexpQuerySetFieldTest1() {
        query(regexp("set_1", "")).check(0);
    }

    @Test
    public void regexpQuerySetFieldTest2() {
        query(regexp("set_1", "l.*")).check(0);
    }

    @Test
    public void regexpQuerySetFieldTest3() {
        query(regexp("set_1", "s.*")).check(5);
    }

    @Test
    public void regexpQueryMapFieldTest1() {
        query(regexp("map_1$k1", "")).check(0);
    }

    @Test
    public void regexpQueryMapFieldTest2() {
        query(regexp("map_1$k1", "l.*")).check(0);
    }

    @Test
    public void regexpQueryMapFieldTest3() {
        query(regexp("map_1$k1", "k.*")).check(0);
    }

    @Test
    public void regexpQueryMapFieldTest4() {
        query(regexp("map_1$k1", "v.*")).check(2);
    }

    @Test
    public void regexpFilterAsciiFieldTest1() {
        filter(regexp("ascii_1", "frase.*")).check(4);
    }

    @Test
    public void regexpFilterAsciiFieldTest2() {
        filter(regexp("ascii_1", "frase .*")).check(1);
    }

    @Test
    public void regexpFilterAsciiFieldTest3() {
        filter(regexp("ascii_1", ".*")).check(5);
    }

    @Test
    public void regexpFilterAsciiFieldTest4() {
        filter(regexp("ascii_1", "")).check(0);
    }

    @Test
    public void regexpFilterAsciiFieldTest5() {
        filter(regexp("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void regexpFilterInetFieldTest1() {
        filter(regexp("inet_1", ".*")).check(5);
    }

    @Test
    public void regexpFilterInetFieldTest2() {
        filter(regexp("inet_1", "127.*")).check(4);
    }

    @Test
    public void regexpFilterInetFieldTest3() {
        filter(regexp("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void regexpFilterInetFieldTest4() {
        filter(regexp("inet_1", "")).check(0);
    }

    @Test
    public void regexpFilterInetFieldTest5() {
        filter(regexp("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void regexpFilterTextFieldTest1() {
        filter(regexp("text_1", ".*")).check(5);
    }

    @Test
    public void regexpFilterTextFieldTest2() {
        filter(regexp("text_1", "frase.*")).check(4);
    }

    @Test
    public void regexpFilterTextFieldTest3() {
        filter(regexp("text_1", "frase .*")).check(0);
    }

    @Test
    public void regexpFilterTextFieldTest4() {
        filter(regexp("text_1", "")).check(0);
    }

    @Test
    public void regexpFilterTextFieldTest5() {
        filter(regexp("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void regexpFilterVarcharFieldTest1() {
        filter(regexp("varchar_1", ".*")).check(5);
    }

    @Test
    public void regexpFilterVarcharFieldTest2() {
        filter(regexp("varchar_1", "frase.*")).check(4);
    }

    @Test
    public void regexpFilterVarcharFieldTest3() {
        filter(regexp("varchar_1", "frase .*")).check(1);
    }

    @Test
    public void regexpFilterVarcharFieldTest4() {
        filter(regexp("varchar_1", "")).check(0);
    }

    @Test
    public void regexpFilterVarcharFieldTest5() {
        filter(regexp("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void regexpFilterListFieldTest1() {
        filter(regexp("list_1", "")).check(0);
    }

    @Test
    public void regexpFilterListFieldTest2() {
        filter(regexp("list_1", "l.*")).check(5);
    }

    @Test
    public void regexpFilterListFieldTest3() {
        filter(regexp("list_1", "s.*")).check(0);
    }

    @Test
    public void regexpFilterSetFieldTest1() {
        filter(regexp("set_1", "")).check(0);
    }

    @Test
    public void regexpFilterSetFieldTest2() {
        filter(regexp("set_1", "l.*")).check(0);
    }

    @Test
    public void regexpFilterSetFieldTest3() {
        filter(regexp("set_1", "s.*")).check(5);
    }

    @Test
    public void regexpFilterMapFieldTest1() {
        filter(regexp("map_1$k1", "")).check(0);
    }

    @Test
    public void regexpFilterMapFieldTest2() {
        filter(regexp("map_1$k1", "l.*")).check(0);
    }

    @Test
    public void regexpFilterMapFieldTest3() {
        filter(regexp("map_1$k1", "k.*")).check(0);
    }

    @Test
    public void regexpFilterMapFieldTest4() {
        filter(regexp("map_1$k1", "v.*")).check(2);
    }
}
