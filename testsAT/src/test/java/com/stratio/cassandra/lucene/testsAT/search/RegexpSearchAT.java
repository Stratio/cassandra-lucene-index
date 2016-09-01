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

import static com.stratio.cassandra.lucene.builder.Builder.regexp;

@RunWith(JUnit4.class)
public class RegexpSearchAT extends AbstractSearchAT {

    @Test
    public void testRegexpAsciiField1() {
        filter(regexp("ascii_1", "frase.*")).check(4);
    }

    @Test
    public void testRegexpAsciiField2() {
        filter(regexp("ascii_1", "frase .*")).check(1);
    }

    @Test
    public void testRegexpAsciiField3() {
        filter(regexp("ascii_1", ".*")).check(5);
    }

    @Test
    public void testRegexpAsciiField4() {
        filter(regexp("ascii_1", "")).check(0);
    }

    @Test
    public void testRegexpAsciiField5() {
        filter(regexp("ascii_1", "frase tipo ascii")).check(1);
    }

    @Test
    public void testRegexpInetField1() {
        filter(regexp("inet_1", ".*")).check(5);
    }

    @Test
    public void testRegexpInetField2() {
        filter(regexp("inet_1", "127.*")).check(4);
    }

    @Test
    public void testRegexpInetField3() {
        filter(regexp("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void testRegexpInetField4() {
        filter(regexp("inet_1", "")).check(0);
    }

    @Test
    public void testRegexpInetField5() {
        filter(regexp("inet_1", "127.1.1.1")).check(2);
    }

    @Test
    public void testRegexpTextField1() {
        filter(regexp("text_1", ".*")).check(5);
    }

    @Test
    public void testRegexpTextField2() {
        filter(regexp("text_1", "frase.*")).check(4);
    }

    @Test
    public void testRegexpTextField3() {
        filter(regexp("text_1", "frase .*")).check(0);
    }

    @Test
    public void testRegexpTextField4() {
        filter(regexp("text_1", "")).check(0);
    }

    @Test
    public void testRegexpTextField5() {
        filter(regexp("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void testRegexpVarcharField1() {
        filter(regexp("varchar_1", ".*")).check(5);
    }

    @Test
    public void testRegexpVarcharField2() {
        filter(regexp("varchar_1", "frase.*")).check(4);
    }

    @Test
    public void testRegexpVarcharField3() {
        filter(regexp("varchar_1", "frase .*")).check(1);
    }

    @Test
    public void testRegexpVarcharField4() {
        filter(regexp("varchar_1", "")).check(0);
    }

    @Test
    public void testRegexpVarcharField5() {
        filter(regexp("varchar_1", "frasesencillasinespacios")).check(1);
    }

    @Test
    public void testRegexpListField1() {
        filter(regexp("list_1", "")).check(0);
    }

    @Test
    public void testRegexpListField2() {
        filter(regexp("list_1", "l.*")).check(5);
    }

    @Test
    public void testRegexpListField3() {
        filter(regexp("list_1", "s.*")).check(0);
    }

    @Test
    public void testRegexpSetField1() {
        filter(regexp("set_1", "")).check(0);
    }

    @Test
    public void testRegexpSetField2() {
        filter(regexp("set_1", "l.*")).check(0);
    }

    @Test
    public void testRegexpSetField3() {
        filter(regexp("set_1", "s.*")).check(5);
    }

    @Test
    public void testRegexpMapField1() {
        filter(regexp("map_1$k1", "")).check(0);
    }

    @Test
    public void testRegexpMapField2() {
        filter(regexp("map_1$k1", "l.*")).check(0);
    }

    @Test
    public void testRegexpMapField3() {
        filter(regexp("map_1$k1", "k.*")).check(0);
    }

    @Test
    public void testRegexpMapField4() {
        filter(regexp("map_1$k1", "v.*")).check(2);
    }

    @Test
    public void testRegexpMapFieldWithAlias1() {
        filter(regexp("string_map$k1", "")).check(0);
    }

    @Test
    public void testRegexpMapFieldWithAlias2() {
        filter(regexp("string_map$k1", "l.*")).check(0);
    }

    @Test
    public void testRegexpMapFieldWithAlias3() {
        filter(regexp("string_map$k1", "k.*")).check(0);
    }

    @Test
    public void testRegexpMapFieldWithAlias4() {
        filter(regexp("string_map$k1", "v.*")).check(2);
    }
}
