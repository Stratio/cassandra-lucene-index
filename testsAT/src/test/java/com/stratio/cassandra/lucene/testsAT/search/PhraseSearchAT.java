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

import static com.stratio.cassandra.lucene.builder.Builder.phrase;

@RunWith(JUnit4.class)
public class PhraseSearchAT extends AbstractSearchAT {

    @Test
    public void phraseQueryTextFieldTest1() {
        query(phrase("text_1", "Frase espacios")).check(0);
    }

    @Test
    public void phraseQueryTextFieldWithSlopTest1() {
        query(phrase("text_1", "Frase espacios").slop(2)).check(1);
    }

    @Test
    public void phraseQueryTextFieldTest2() {
        query(phrase("text_1", "articulos suficientes")).check(0);
    }

    @Test
    public void phraseQueryTextFieldWithSlopTest() {
        query(phrase("text_1", "articulos palabras").slop(2)).check(1);
    }

    @Test
    public void phraseQueryTextFieldTest3() {
        query(phrase("text_1", "con los")).check(0);
    }

    @Test
    public void phraseQueryTextFieldTest4() {
        query(phrase("text_1", "")).check(0);
    }

    @Test
    public void phraseQueryListFieldTest1() {
        query(phrase("list_1", "")).check(0);
    }

    @Test
    public void phraseQueryListFieldTest2() {
        query(phrase("list_1", "l1")).check(2);
    }

    @Test
    public void phraseQueryListFieldTest3() {
        query(phrase("list_1", "l1 l2")).check(1);
    }

    @Test
    public void phraseQueryListFieldTest4() {
        query(phrase("list_1", "s1")).check(0);
    }

    @Test
    public void phraseQueryListFieldTest5() {
        query(phrase("list_1", "l2 l3")).check(3);
    }

    @Test
    public void phraseQueryListFieldTest6() {
        query(phrase("list_1", "l3 l2")).check(0);
    }

    @Test
    public void phraseQuerySetFieldTest1() {
        query(phrase("set_1", "")).check(0);
    }

    @Test
    public void phraseQuerySetFieldTest2() {
        query(phrase("set_1", "l1")).check(0);
    }

    @Test
    public void phraseQuerySetFieldTest3() {
        query(phrase("set_1", "s1")).check(2);
    }

    @Test
    public void phraseQueryMapFieldTest1() {
        query(phrase("map_1$k1", "")).check(0);
    }

    @Test
    public void phraseQueryMapFieldTest2() {
        query(phrase("map_1$k1", "l1")).check(0);
    }

    @Test
    public void phraseQueryMapFieldTest3() {
        query(phrase("map_1$k1", ("k1"))).check(0);
    }

    @Test
    public void phraseQueryMapFieldTest4() {
        query(phrase("map_1$k1", ("v1"))).check(2);
    }

    @Test
    public void phraseFilterTextFieldTest1() {
        filter(phrase("text_1", "Frase espacios")).check(0);
    }

    @Test
    public void phraseFilterTextFieldWithSlopTest1() {
        filter(phrase("text_1", "Frase espacios").slop(2)).check(1);
    }

    @Test
    public void phraseFilterTextFieldTest2() {
        filter(phrase("text_1", "articulos suficientes")).check(0);
    }

    @Test
    public void phraseFilterTextFieldWithSlopTest() {
        filter(phrase("text_1", "articulos palabras").slop(2)).check(1);
    }

    @Test
    public void phraseFilterTextFieldTest3() {
        filter(phrase("text_1", "con los")).check(0);
    }

    @Test
    public void phraseFilterTextFieldTest4() {
        filter(phrase("text_1", "")).check(0);
    }

    @Test
    public void phraseFilterListFieldTest1() {
        filter(phrase("list_1", "")).check(0);
    }

    @Test
    public void phraseFilterListFieldTest2() {
        filter(phrase("list_1", "l1")).check(2);
    }

    @Test
    public void phraseFilterListFieldTest3() {
        filter(phrase("list_1", "l1 l2")).check(1);
    }

    @Test
    public void phraseFilterListFieldTest4() {
        filter(phrase("list_1", "s1")).check(0);
    }

    @Test
    public void phraseFilterListFieldTest5() {
        filter(phrase("list_1", "l2 l3")).check(3);
    }

    @Test
    public void phraseFilterListFieldTest6() {
        filter(phrase("list_1", "l3 l2")).check(0);
    }

    @Test
    public void phraseFilterSetFieldTest1() {
        filter(phrase("set_1", "")).check(0);
    }

    @Test
    public void phraseFilterSetFieldTest2() {
        filter(phrase("set_1", "l1")).check(0);
    }

    @Test
    public void phraseFilterSetFieldTest3() {
        filter(phrase("set_1", "s1")).check(2);
    }

    @Test
    public void phraseFilterMapFieldTest1() {
        filter(phrase("map_1$k1", "")).check(0);
    }

    @Test
    public void phraseFilterMapFieldTest2() {
        filter(phrase("map_1$k1", "l1")).check(0);
    }

    @Test
    public void phraseFilterMapFieldTest3() {
        filter(phrase("map_1$k1", ("k1"))).check(0);
    }

    @Test
    public void phraseFilterMapFieldTest4() {
        filter(phrase("map_1$k1", ("v1"))).check(2);
    }
}