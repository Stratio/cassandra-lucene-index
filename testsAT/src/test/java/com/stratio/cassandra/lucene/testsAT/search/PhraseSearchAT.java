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
package com.stratio.cassandra.lucene.testsAT.search;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.phrase;

@RunWith(JUnit4.class)
public class PhraseSearchAT extends AbstractSearchAT {

    @Test
    public void phraseTextFieldTest1() {
        filter(phrase("text_1", "Frase espacios")).check(0);
    }

    @Test
    public void phraseTextFieldWithSlopTest1() {
        filter(phrase("text_1", "Frase espacios").slop(2)).check(1);
    }

    @Test
    public void phraseTextFieldTest2() {
        filter(phrase("text_1", "articulos suficientes")).check(0);
    }

    @Test
    public void phraseTextFieldWithSlopTest() {
        filter(phrase("text_1", "articulos palabras").slop(2)).check(1);
    }

    @Test
    public void phraseTextFieldTest3() {
        filter(phrase("text_1", "con los")).check(0);
    }

    @Test
    public void phraseTextFieldTest4() {
        filter(phrase("text_1", "")).check(0);
    }

    @Test
    public void phraseListFieldTest1() {
        filter(phrase("list_1", "")).check(0);
    }

    @Test
    public void phraseListFieldTest2() {
        filter(phrase("list_1", "l1")).check(2);
    }

    @Test
    public void phraseListFieldTest3() {
        filter(phrase("list_1", "l1 l2")).check(1);
    }

    @Test
    public void phraseListFieldTest4() {
        filter(phrase("list_1", "s1")).check(0);
    }

    @Test
    public void phraseListFieldTest5() {
        filter(phrase("list_1", "l2 l3")).check(3);
    }

    @Test
    public void phraseListFieldTest6() {
        filter(phrase("list_1", "l3 l2")).check(0);
    }

    @Test
    public void phraseSetFieldTest1() {
        filter(phrase("set_1", "")).check(0);
    }

    @Test
    public void phraseSetFieldTest2() {
        filter(phrase("set_1", "l1")).check(0);
    }

    @Test
    public void phraseSetFieldTest3() {
        filter(phrase("set_1", "s1")).check(2);
    }

    @Test
    public void phraseMapFieldTest1() {
        filter(phrase("map_1$k1", "")).check(0);
    }

    @Test
    public void phraseMapFieldTest2() {
        filter(phrase("map_1$k1", "l1")).check(0);
    }

    @Test
    public void phraseMapFieldTest3() {
        filter(phrase("map_1$k1", ("k1"))).check(0);
    }

    @Test
    public void phraseMapFieldTest4() {
        filter(phrase("map_1$k1", ("v1"))).check(2);
    }

    @Test
    public void phraseMapFieldTestWithAlias1() {
        filter(phrase("string_map$k1", "")).check(0);
    }

    @Test
    public void phraseMapFieldTestWithAlias2() {
        filter(phrase("string_map$k1", "l1")).check(0);
    }

    @Test
    public void phraseMapFieldTestWithAlias3() {
        filter(phrase("string_map$k1", ("k1"))).check(0);
    }

    @Test
    public void phraseMapFieldTestWithAlias4() {
        filter(phrase("string_map$k1", ("v1"))).check(2);
    }
}