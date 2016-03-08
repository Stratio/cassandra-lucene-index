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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.fuzzy;

@RunWith(JUnit4.class)
public class FuzzySearchAT extends AbstractSearchAT {

    @Test
    public void fuzzyFilterAsciiFieldTest() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyFilterEmptyAsciiFieldTest() {
        filter(fuzzy("ascii_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith1MaxEditsTest() {
        filter(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith0MaxEditsTest() {
        filter(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith2PrefixLengthTest1() {
        filter(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith2PrefixLengthTest2() {
        filter(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith10MaxExpansionsTest() {
        filter(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyFilterAsciiFieldWithoutTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyFilterAsciiFieldWithTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyFilterInetFieldTest() {
        filter(fuzzy("inet_1", "127.0.1.1")).check(4);
    }

    @Test
    public void fuzzyFilterEmptyInetFieldTest() {
        filter(fuzzy("inet_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterInetFieldWith1MaxEditsTest() {
        filter(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyFilterInetFieldWith0MaxEditsTest() {
        filter(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).check(1);
    }

    @Test
    public void fuzzyFilterInetFieldWith2PrefixLengthTest1() {
        filter(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).check(4);
    }

    @Test
    public void fuzzyFilterInetFieldWith2PrefixLengthTest2() {
        filter(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyFilterInetFieldWith10MaxExpansionsTest() {
        filter(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).check(4);
    }

    @Test
    public void fuzzyFilterInetFieldWithoutTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(false)).check(3);
    }

    @Test
    public void fuzzyFilterInetFieldWithTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").transpositions(true)).check(4);
    }

    @Test
    public void fuzzyFilterInetFieldWith1MaxEditsAndTranspositionsTest() {
        filter(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).check(1);
    }

    @Test
    public void fuzzyFilterTextFieldTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).check(2);
    }

    @Test
    public void fuzzyFilterEmptyTextFieldTest() {
        filter(fuzzy("text_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterTextFieldWith1MaxEditsTest() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1)).check(1);
    }

    @Test
    public void fuzzyFilterTextFieldWith0MaxEditsTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyFilterTextFieldWith2PrefixLengthTest1() {
        filter(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyFilterTextFieldWith2PrefixLengthTest2() {
        filter(fuzzy("text_1", "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyFilterTextFieldWith10MaxExpansionsTest() {
        filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyFilterTextFieldWithoutTranspositionsTest() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyFilterTextFieldWithTranspositionsTest() {
        filter(fuzzy("text_1", "frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyFilterTextFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                     .transpositions(true)).check(0);
    }

    @Test
    public void fuzzyFilterVarcharFieldTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).check(3);
    }

    @Test
    public void fuzzyFilterEmptyVarcharFieldTest() {
        filter(fuzzy("varchar_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith1MaxEditsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith0MaxEditsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith2PrefixLengthTest1() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2)).check(2);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith2PrefixLengthTest2() {
        filter(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith1MaxExpansionsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(1)).check(2);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith10MaxExpansionsTest() {
        filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10)).check(3);
    }

    @Test
    public void fuzzyFilterVarcharFieldWithoutTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyFilterVarcharFieldWithTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true)).check(2);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith5MaxEditsAndTranspositionsTest() {
        filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyFilterListFieldTest1() {
        filter(fuzzy("list_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterListFieldTest2() {
        filter(fuzzy("list_1", "l1")).check(5);
    }

    @Test
    public void fuzzyFilterListFieldTest3() {
        filter(fuzzy("list_1", "s1")).check(2);
    }

    @Test
    public void fuzzyFilterListFieldTest4() {
        filter(fuzzy("list_1", "s7l")).check(0);
    }

    @Test
    public void fuzzyFilterSetFieldTest1() {
        filter(fuzzy("set_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterSetFieldTest2() {
        filter(fuzzy("set_1", "l1")).check(2);
    }

    @Test
    public void fuzzyFilterSetFieldTest3() {
        filter(fuzzy("set_1", "s1")).check(5);
    }

    @Test
    public void fuzzyFilterSetFieldTest4() {
        filter(fuzzy("set_1", "k87")).check(0);
    }

    @Test
    public void fuzzyFilterMapFieldTest1() {
        filter(fuzzy("map_1$k1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyFilterMapFieldTest2() {
        filter(fuzzy("map_1$k1", "l1")).check(2);
    }

    @Test
    public void fuzzyFilterMapFieldTest3() {
        filter(fuzzy("map_1$k1", "k1")).check(2);
    }

    @Test
    public void fuzzyFilterMapFieldTest4() {
        filter(fuzzy("map_1$k1", "v1")).check(2);
    }

    @Test
    public void fuzzyQueryAsciiFieldTest() {
        query(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyQueryEmptyAsciiFieldTest() {
        query(fuzzy("ascii_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith1MaxEditsTest() {
        query(fuzzy("ascii_1", "frase tipo asci")).check(2);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith0MaxEditsTest() {
        query(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith2PrefixLengthTest1() {
        query(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith2PrefixLengthTest2() {
        query(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith10MaxExpansionsTest() {
        query(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyQueryAsciiFieldWithoutTranspositionsTest() {
        query(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyQueryAsciiFieldWithTranspositionsTest() {
        query(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith5MaxEditsAndTranspositionsTest() {
        query(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyQueryInetFieldTest() {
        query(fuzzy("inet_1", "127.0.1.1")).check(4);
    }

    @Test
    public void fuzzyQueryEmptyInetFieldTest() {
        query(fuzzy("inet_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyQueryInetFieldWith1MaxEditsTest() {
        query(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyQueryInetFieldWith0MaxEditsTest() {
        query(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).check(1);
    }

    @Test
    public void fuzzyQueryInetFieldWith2PrefixLengthTest1() {
        query(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).check(4);
    }

    @Test
    public void fuzzyQueryInetFieldWith2PrefixLengthTest2() {
        query(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyQueryInetFieldWith10MaxExpansionsTest() {
        query(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).check(4);
    }

    @Test
    public void fuzzyQueryInetFieldWithoutTranspositionsTest() {
        query(fuzzy("inet_1", "1270..1.1").transpositions(false)).check(3);
    }

    @Test
    public void fuzzyQueryInetFieldWithTranspositionsTest() {
        query(fuzzy("inet_1", "1270..1.1").transpositions(true)).check(4);
    }

    @Test
    public void fuzzyQueryInetFieldWith1MaxEditsAndTranspositionsTest() {
        query(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).check(1);
    }

    @Test
    public void fuzzyQueryTextFieldTest() {
        query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).check(2);
    }

    @Test
    public void fuzzyQueryEmptyTextFieldTest() {
        query(fuzzy("text_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyQueryTextFieldWith1MaxEditsTest() {
        query(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1)).check(1);
    }

    @Test
    public void fuzzyQueryTextFieldWith0MaxEditsTest() {
        query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyQueryTextFieldWith2PrefixLengthTest1() {
        query(fuzzy("text_1", "frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(1);
    }

    @Test
    public void fuzzyQueryTextFieldWith2PrefixLengthTest2() {
        query(fuzzy("text_1", "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyQueryTextFieldWith10MaxExpansionsTest() {
        query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10)).check(2);
    }

    @Test
    public void fuzzyQueryTextFieldWithoutTranspositionsTest() {
        query(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyQueryTextFieldWithTranspositionsTest() {
        query(fuzzy("text_1", "frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true)).check(1);
    }

    @Test
    public void fuzzyQueryTextFieldWith5MaxEditsAndTranspositionsTest() {
        query(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                    .transpositions(true)).check(0);
    }

    @Test
    public void fuzzyQueryVarcharFieldTest() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).check(3);
    }

    @Test
    public void fuzzyQueryEmptyVarcharFieldTest() {
        query(fuzzy("varchar_1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith1MaxEditsTest() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).check(2);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith0MaxEditsTest() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).check(0);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith2PrefixLengthTest1() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2)).check(2);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith2PrefixLengthTest2() {
        query(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2)).check(0);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith1MaxExpansionsTest() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(1)).check(2);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith10MaxExpansionsTest() {
        query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10)).check(3);
    }

    @Test
    public void fuzzyQueryVarcharFieldWithoutTranspositionsTest() {
        query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false)).check(0);
    }

    @Test
    public void fuzzyQueryVarcharFieldWithTranspositionsTest() {
        query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true)).check(2);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith5MaxEditsAndTranspositionsTest() {
        query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1).transpositions(true)).check(0);
    }

    @Test
    public void fuzzyQueryListFieldTest1() {
        query(fuzzy("list_1", "")).check(Exception.class);
    }

    @Test
    public void fuzzyQueryListFieldTest2() {
        query(fuzzy("list_1", "l1")).check(5);
    }

    @Test
    public void fuzzyQueryListFieldTest3() {
        query(fuzzy("list_1", "s1")).check(2);
    }

    @Test
    public void fuzzyQueryListFieldTest4() {
        query(fuzzy("list_1", "s7l")).check(0);
    }

    @Test
    public void fuzzyQuerySetFieldTest1() {
        query(fuzzy("set_1", "")).check(Exception.class);
    }

    @Test
    public void fuzzyQuerySetFieldTest2() {
        query(fuzzy("set_1", "l1")).check(2);
    }

    @Test
    public void fuzzyQuerySetFieldTest3() {
        query(fuzzy("set_1", "s1")).check(5);
    }

    @Test
    public void fuzzyQuerySetFieldTest4() {
        query(fuzzy("set_1", "k87")).check(0);
    }

    @Test
    public void fuzzyQueryMapFieldTest1() {
        query(fuzzy("map_1$k1", "")).check(InvalidQueryException.class);
    }

    @Test
    public void fuzzyQueryMapFieldTest2() {
        query(fuzzy("map_1$k1", "l1")).check(2);
    }

    @Test
    public void fuzzyQueryMapFieldTest3() {
        query(fuzzy("map_1$k1", "k1")).check(2);
    }

    @Test
    public void fuzzyQueryMapFieldTest4() {
        query(fuzzy("map_1$k1", "v1")).check(2);
    }
}
