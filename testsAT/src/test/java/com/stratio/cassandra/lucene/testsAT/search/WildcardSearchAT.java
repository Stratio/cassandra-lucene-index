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

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;

@RunWith(JUnit4.class)
public class WildcardSearchAT extends AbstractSearchAT {

    @Test
    public void wildcardQueryAsciiFieldTest1() {
        query(wildcard("ascii_1", "*")).check(5);
    }

    @Test
    public void wildcardQueryAsciiFieldTest2() {
        query(wildcard("ascii_1", "frase*")).check(4);
    }

    @Test
    public void wildcardQueryAsciiFieldTest3() {
        query(wildcard("ascii_1", "frase *")).check(1);
    }

    @Test
    public void wildcardQueryAsciiFieldTest4() {
        query(wildcard("ascii_1", "")).check(0);
    }

    @Test
    public void wildcardQueryInetFieldTest1() {
        query(wildcard("inet_1", "*")).check(5);
    }

    @Test
    public void wildcardQueryInetFieldTest2() {
        query(wildcard("inet_1", "127*")).check(4);
    }

    @Test
    public void wildcardQueryInetFieldTest3() {
        query(wildcard("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void wildcardQueryInetFieldTest4() {
        query(wildcard("inet_1", "")).check(0);
    }

    @Test
    public void wildcardQueryTextFieldTest1() {
        query(wildcard("text_1", "*")).check(5);
    }

    @Test
    public void wildcardQueryTextFieldTest2() {
        query(wildcard("text_1", "Frase*")).check(0);
    }

    @Test
    public void wildcardQueryTextFieldTest3() {
        query(wildcard("text_1", "Frasesin*")).check(0);
    }

    @Test
    public void wildcardQueryTextFieldTest4() {
        query(wildcard("text_1", "")).check(0);
    }

    @Test
    public void wildcardQueryVarcharFieldTest1() {
        query(wildcard("varchar_1", "*")).check(5);
    }

    @Test
    public void wildcardQueryVarcharFieldTest2() {
        query(wildcard("varchar_1", "frase*")).check(4);
    }

    @Test
    public void wildcardQueryVarcharFieldTest3() {
        query(wildcard("varchar_1", "frase sencilla*")).check(1);
    }

    @Test
    public void wildcardQueryVarcharFieldTest4() {
        query(wildcard("varchar_1", "")).check(0);
    }

    @Test
    public void wildcardFilterAsciiFieldTest1() {
        filter(wildcard("ascii_1", "*")).check(5);
    }

    @Test
    public void wildcardFilterAsciiFieldTest2() {
        filter(wildcard("ascii_1", "frase*")).check(4);
    }

    @Test
    public void wildcardFilterAsciiFieldTest3() {
        filter(wildcard("ascii_1", "frase *")).check(1);
    }

    @Test
    public void wildcardFilterAsciiFieldTest4() {
        filter(wildcard("ascii_1", "")).check(0);
    }

    @Test
    public void wildcardFilterInetFieldTest1() {
        filter(wildcard("inet_1", "*")).check(5);
    }

    @Test
    public void wildcardFilterInetFieldTest2() {
        filter(wildcard("inet_1", "127*")).check(4);
    }

    @Test
    public void wildcardFilterInetFieldTest3() {
        filter(wildcard("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void wildcardFilterInetFieldTest4() {
        filter(wildcard("inet_1", "")).check(0);
    }

    @Test
    public void wildcardFilterTextFieldTest1() {
        filter(wildcard("text_1", "*")).check(5);
    }

    @Test
    public void wildcardFilterTextFieldTest2() {
        filter(wildcard("text_1", "Frase*")).check(0);
    }

    @Test
    public void wildcardFilterTextFieldTest3() {
        filter(wildcard("text_1", "Frasesin*")).check(0);
    }

    @Test
    public void wildcardFilterTextFieldTest4() {
        filter(wildcard("text_1", "")).check(0);
    }

    @Test
    public void wildcardFilterVarcharFieldTest1() {
        filter(wildcard("varchar_1", "*")).check(5);
    }

    @Test
    public void wildcardFilterVarcharFieldTest2() {
        filter(wildcard("varchar_1", "frase*")).check(4);
    }

    @Test
    public void wildcardFilterVarcharFieldTest3() {
        filter(wildcard("varchar_1", "frase sencilla*")).check(1);
    }

    @Test
    public void wildcardFilterVarcharFieldTest4() {
        filter(wildcard("varchar_1", "")).check(0);
    }

    @Test
    public void wildcardFilteredQueryTextFieldTest1() {
        query(wildcard("varchar_1", "frase*")).filter(wildcard("text_1", "*")).check(4);
    }

    @Test
    public void wildcardFilteredQueryTextFieldTest2() {
        query(wildcard("text_1", "*")).filter(wildcard("varchar_1", "frase*")).check(4);
    }

    @Test
    public void wildcardQueryListFieldTest1() {
        query(wildcard("list_1", "")).check(0);
    }

    @Test
    public void wildcardQueryListFieldTest2() {
        query(wildcard("list_1", "l*")).check(5);
    }

    @Test
    public void wildcardQueryListFieldTest3() {
        query(wildcard("list_1", "s*")).check(0);
    }

    @Test
    public void wildcardQuerySetFieldTest1() {
        query(wildcard("set_1", "")).check(0);
    }

    @Test
    public void wildcardQuerySetFieldTest2() {
        query(wildcard("set_1", "l*")).check(0);
    }

    @Test
    public void wildcardQuerySetFieldTest3() {
        query(wildcard("set_1", "s*")).check(5);
    }

    @Test
    public void wildcardQueryMapFieldTest1() {
        query(wildcard("map_1$k1", "")).check(0);
    }

    @Test
    public void wildcardQueryMapFieldTest2() {
        query(wildcard("map_1$k1", "l*")).check(0);
    }

    @Test
    public void wildcardQueryMapFieldTest3() {
        query(wildcard("map_1$k1", "k*")).check(0);
    }

    @Test
    public void wildcardQueryMapFieldTest4() {
        query(wildcard("map_1$k1", "v*")).check(2);
    }

}
