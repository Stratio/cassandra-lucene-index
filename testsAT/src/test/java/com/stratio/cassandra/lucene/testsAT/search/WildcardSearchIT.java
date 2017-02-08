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

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;

@RunWith(JUnit4.class)
public class WildcardSearchIT extends AbstractSearchIT {

    @Test
    public void testWildcardAsciiField1() {
        filter(wildcard("ascii_1", "*")).check(5);
    }

    @Test
    public void testWildcardAsciiField2() {
        filter(wildcard("ascii_1", "frase*")).check(4);
    }

    @Test
    public void testWildcardAsciiField3() {
        filter(wildcard("ascii_1", "frase *")).check(1);
    }

    @Test
    public void testWildcardAsciiField4() {
        filter(wildcard("ascii_1", "")).check(0);
    }

    @Test
    public void testWildcardInetField1() {
        filter(wildcard("inet_1", "*")).check(5);
    }

    @Test
    public void testWildcardInetField2() {
        filter(wildcard("inet_1", "127*")).check(4);
    }

    @Test
    public void testWildcardInetField3() {
        filter(wildcard("inet_1", "127.1.*")).check(2);
    }

    @Test
    public void testWildcardInetField4() {
        filter(wildcard("inet_1", "")).check(0);
    }

    @Test
    public void testWildcardTextField1() {
        filter(wildcard("text_1", "*")).check(5);
    }

    @Test
    public void testWildcardTextField2() {
        filter(wildcard("text_1", "Frase*")).check(0);
    }

    @Test
    public void testWildcardTextField3() {
        filter(wildcard("text_1", "Frasesin*")).check(0);
    }

    @Test
    public void testWildcardTextField4() {
        filter(wildcard("text_1", "")).check(0);
    }

    @Test
    public void testWildcardVarcharField1() {
        filter(wildcard("varchar_1", "*")).check(5);
    }

    @Test
    public void testWildcardVarcharField2() {
        filter(wildcard("varchar_1", "frase*")).check(4);
    }

    @Test
    public void testWildcardVarcharField3() {
        filter(wildcard("varchar_1", "frase sencilla*")).check(1);
    }

    @Test
    public void testWildcardVarcharField4() {
        filter(wildcard("varchar_1", "")).check(0);
    }
}
