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

import static com.stratio.cassandra.lucene.builder.Builder.prefix;

@RunWith(JUnit4.class)
public class PrefixSearchAT extends AbstractSearchAT {

    @Test
    public void testPrefixAsciiField1() {
        filter(prefix("ascii_1", "frase ")).check(1);
    }

    @Test
    public void testPrefixAsciiField2() {
        filter(prefix("ascii_1", "frase")).check(4);
    }

    @Test
    public void testPrefixAsciiField3() {
        filter(prefix("ascii_1", "F")).check(0);
    }

    @Test
    public void testPrefixAsciiField4() {
        filter(prefix("ascii_1", "")).check(5);
    }

    @Test
    public void testPrefixInetField1() {
        filter(prefix("inet_1", "127")).check(4);
    }

    @Test
    public void testPrefixInetField2() {
        filter(prefix("inet_1", "")).check(5);
    }

    @Test
    public void testPrefixInetField3() {
        filter(prefix("inet_1", "127.0.")).check(2);
    }

    @Test
    public void testPrefixTextField1() {
        filter(prefix("text_1", "Frase con espacios articulos y las palabras suficientes")).check(0);
    }

    @Test
    public void testPrefixTextField2() {
        filter(prefix("text_1", "Frase")).check(0);
    }

    @Test
    public void testPrefixTextField3() {
        filter(prefix("text_1", "")).check(5);
    }

    @Test
    public void testPrefixVarcharField1() {
        filter(prefix("varchar_1", "frasesencillasinespaciosperomaslarga")).check(2);
    }

    @Test
    public void testPrefixVarcharField2() {
        filter(prefix("varchar_1", "frase")).check(4);
    }

    @Test
    public void testPrefixVarcharField3() {
        filter(prefix("varchar_1", "")).check(5);
    }

    @Test
    public void testPrefixListField1() {
        filter(prefix("list_1", "")).check(5);
    }

    @Test
    public void testPrefixListField2() {
        filter(prefix("list_1", "l1")).check(2);
    }

    @Test
    public void testPrefixListField3() {
        filter(prefix("list_1", "l")).check(5);
    }

    @Test
    public void testPrefixListField4() {
        filter(prefix("list_1", "s1")).check(0);
    }

    @Test
    public void testPrefixSetField1() {
        filter(prefix("set_1", "")).check(5);
    }

    @Test
    public void testPrefixSetField2() {
        filter(prefix("set_1", "l1")).check(0);
    }

    @Test
    public void testPrefixSetField3() {
        filter(prefix("set_1", "s1")).check(2);
    }

    @Test
    public void testPrefixMapField1() {
        filter(prefix("map_1$k1", "")).check(2);
    }

    @Test
    public void testPrefixMapField2() {
        filter(prefix("map_1$k1", "l1")).check(0);
    }

    @Test
    public void testPrefixMapField3() {
        filter(prefix("map_1$k1", "k1")).check(0);
    }

    @Test
    public void testPrefixMapField4() {
        filter(prefix("map_1$k1", "v1")).check(2);
    }

    @Test
    public void testPrefixMapFieldWithAlias1() {
        filter(prefix("string_map$k1", "")).check(2);
    }

    @Test
    public void testPrefixMapFieldWithAlias2() {
        filter(prefix("string_map$k1", "l1")).check(0);
    }

    @Test
    public void testPrefixMapFieldWithAlias3() {
        filter(prefix("string_map$k1", "k1")).check(0);
    }

    @Test
    public void testPrefixMapFieldWithAlias4() {
        filter(prefix("string_map$k1", "v1")).check(2);
    }
}
