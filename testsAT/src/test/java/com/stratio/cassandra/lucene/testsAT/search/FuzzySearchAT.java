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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.fuzzy;

@RunWith(JUnit4.class)
public class FuzzySearchAT extends AbstractSearchAT {

    @Test
    public void fuzzyAsciiFieldTest() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyEmptyAsciiFieldTest() {
        filter(fuzzy("ascii_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyAsciiFieldWith1MaxEditsTest() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyAsciiFieldWith0MaxEditsTest() {
        filter(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyAsciiFieldWith2PrefixLengthTest1() {
        filter(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyAsciiFieldWith2PrefixLengthTest2() {
        filter(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyAsciiFieldWith10MaxExpansionsTest() {
        filter(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyAsciiFieldWithoutTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyAsciiFieldWithTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyAsciiFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyInetFieldTest() {
        filter(fuzzy("inet_1", "127.0.1.1")).check(4);
    }

    @Test
    public void fuzzyEmptyInetFieldTest() {
        filter(fuzzy("inet_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyInetFieldWith1MaxEditsTest() {
        filter(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyInetFieldWith0MaxEditsTest() {
        filter(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).check(1);
    }

    @Test
    public void fuzzyInetFieldWith2PrefixLengthTest1() {
        filter(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).check(4);
    }

    @Test
    public void fuzzyInetFieldWith2PrefixLengthTest2() {
        filter(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyInetFieldWith10MaxExpansionsTest() {
        filter(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).check(4);
    }

    @Test
    public void fuzzyInetFieldWithoutTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(false)).check(3);
    }

    @Test
    public void fuzzyInetFieldWithTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(true)).check(4);
    }

    @Test
    public void fuzzyInetFieldWith1MaxEditsAndTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).check(1);
    }

    @Test
    public void fuzzyTextFieldTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).check(2);
    }

    @Test
    public void fuzzyEmptyTextFieldTest() {
        filter(fuzzy("text_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyTextFieldWith1MaxEditsTest() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1)).check(1);
    }

    @Test
    public void fuzzyTextFieldWith0MaxEditsTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyTextFieldWith2PrefixLengthTest1() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyTextFieldWith2PrefixLengthTest2() {
        filter(fuzzy("text_1", "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyTextFieldWith10MaxExpansionsTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyTextFieldWithoutTranspositionsTest() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyTextFieldWithTranspositionsTest() {
        filter(fuzzy("text_1", "frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyTextFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                     .transpositions(true)).check(0);
    }

    @Test
    public void fuzzyVarcharFieldTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).check(3);
    }

    @Test
    public void fuzzyEmptyVarcharFieldTest() {
        filter(fuzzy("varchar_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyVarcharFieldWith1MaxEditsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyVarcharFieldWith0MaxEditsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyVarcharFieldWith2PrefixLengthTest1() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2)).check(2);
    }

    @Test
    public void fuzzyVarcharFieldWith2PrefixLengthTest2() {
        filter(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyVarcharFieldWith1MaxExpansionsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(1)).check(2);
    }

    @Test
    public void fuzzyVarcharFieldWith10MaxExpansionsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10)).check(3);
    }

    @Test
    public void fuzzyVarcharFieldWithoutTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyVarcharFieldWithTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true)).check(2);
    }

    @Test
    public void fuzzyVarcharFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyListFieldTest1() {
        filter(fuzzy("list_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyListFieldTest2() {
        filter(fuzzy("list_1", "l1")).check(5);
    }

    @Test
    public void fuzzyListFieldTest3() {
        filter(fuzzy("list_1", "s1")).check(2);
    }

    @Test
    public void fuzzyListFieldTest4() {
        filter(fuzzy("list_1", "s7l")).check(0);
    }

    @Test
    public void fuzzySetFieldTest1() {
        filter(fuzzy("set_1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzySetFieldTest2() {
        filter(fuzzy("set_1", "l1")).check(2);
    }

    @Test
    public void fuzzySetFieldTest3() {
        filter(fuzzy("set_1", "s1")).check(5);
    }

    @Test
    public void fuzzySetFieldTest4() {
        filter(fuzzy("set_1", "k87")).check(0);
    }

    @Test
    public void fuzzyMapFieldTest1() {
        filter(fuzzy("map_1$k1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyMapFieldTest2() {
        filter(fuzzy("map_1$k1", "l1")).check(2);
    }

    @Test
    public void fuzzyMapFieldTest3() {
        filter(fuzzy("map_1$k1", "k1")).check(2);
    }

    @Test
    public void fuzzyMapFieldTest4() {
        filter(fuzzy("map_1$k1", "v1")).check(2);
    }

    @Test
    public void fuzzyMapFieldTestWithAlias1() {
        filter(fuzzy("string_map$k1", "")).check(InvalidQueryException.class, "Field value required");
    }

    @Test
    public void fuzzyMapFieldTestWithAlias2() {
        filter(fuzzy("string_map$k1", "l1")).check(2);
    }

    @Test
    public void fuzzyMapFieldTestWithAlias3() {
        filter(fuzzy("string_map$k1", "k1")).check(2);
    }

    @Test
    public void fuzzyMapFieldTestWithAlias4() {
        filter(fuzzy("string_map$k1", "v1")).check(2);
    }

}
