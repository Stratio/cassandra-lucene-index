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
package com.stratio.cassandra.lucene.testsAT.udt;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTCollectionsAT extends BaseAT {

    private static CassandraUtils cassandraUtils;
    private static final String KEYSPACE_NAME = "udt_collections";

    private static Map<String, String> data1 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER1'");
                put("numbers", "[1,2,3]");
                put("number_set", "{1,2,3}");
                put("number_map", "{'a': 1, 'b': 2 }");
                put("address",
                    "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]");
                put("address_set",
                    "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964}}");
                put("address_map",
                    "{'a': {city:'Barcelona',postcode:28059 }, 'b': {city:'Roma',postcode: 29506}, 'c': {city:'Valencia',postcode:85964 }}");
                put("address_list_list",
                    "[[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                    "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                    "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]]");
                put("address_list_set",
                    "[{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                    "{{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                    "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}]");
                put("address_list_map",
                    "[{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                    "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                    "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}]");
                put("address_set_list",
                    "{[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                    "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                    "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]}");
                put("address_set_set",
                    "{{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                    "{{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                    "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}}");
                put("address_set_map",
                    "{{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                    "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                    "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}}");
            }});
    private static Map<String, String> data2 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER2'");
                put("numbers", "[6, 10, 12]");
                put("number_set", "{6, 10, 12}");
                put("number_map", "{'c':1, 'd':2}");
                put("address",
                    "[{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }]");
                put("address_set",
                    "{{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }}");
                put("address_map", "{'a': {city:'Bilbao',postcode:270548 }," +
                                   " 'b': {city:'Barcelona',postcode:28059 }," +
                                   " 'c': {city:'Venecia',postcode:28756 }} ");
                put("address_list_list", "[" +
                                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                         "[{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]" +
                                         "]");
                put("address_list_set", "[" +
                                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                        "{{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                        "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                        "]");
                put("address_list_map", "[" +
                                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                                        "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                        "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                        "]");
                put("address_set_list", "{" +
                                        "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                        "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                        "[{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]" +
                                        "}");
                put("address_set_set", "{" +
                                       "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                       "{{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                       "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                       "} ");
                put("address_set_map", "{" +
                                       "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                                       "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                       "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                       "}");
            }});
    private static Map<String, String> data3 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER3'");
                put("numbers", "[14,18,20]");
                put("number_set", "{14,18,20}");
                put("number_map", "{'e':1,'f':2}");
                put("address",
                    "[{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }]");
                put("address_set",
                    "{{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }}");
                put("address_map", "{'a': {city:'Lisboa',postcode:29685 }," +
                                   " 'b': {city:'Sevilla',postcode:58964 }," +
                                   " 'c': {city:'Granada',postcode:85964 }}");
                put("address_list_list", "[" +
                                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                         "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                         "[{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                         "]");
                put("address_list_set", "[" +
                                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                        "{{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                        "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                        "]");
                put("address_list_map", "[" +
                                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                        "{'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                        "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}" +
                                        "]");
                put("address_set_list", "{" +
                                        "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                        "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                        "[{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                        "}");
                put("address_set_set", "{" +
                                       "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                       "{{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                       "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                       "}");
                put("address_set_map", "{" +
                                       "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                       "{'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                       "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}" +
                                       "}");
            }});
    private static Map<String, String> data4 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER4'");
                put("numbers", "[3,10,20]");
                put("number_set", "{3,10,20}");
                put("number_map", "{'c':1,'h':2}");
                put("address",
                    "[{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }]");
                put("address_set",
                    "{{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }}");
                put("address_map", "{'a': {city:'Granada',postcode:85964 }," +
                                   " 'b': {city:'Venecia',postcode:28756 }," +
                                   " 'c': {city:'Lisboa',postcode:29685 }}");
                put("address_list_list", "[" +
                                         "[{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                                         "[{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                         "[{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                         "]");
                put("address_list_set", "[" +
                                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                        "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                        "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                        "] ");
                put("address_list_map", "[" +
                                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                                        "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                        "{'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                                        "]");
                put("address_set_list", "{" +
                                        "[{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                                        "[{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                        "[{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                        "}");
                put("address_set_set", "{" +
                                       "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                       "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                       "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                       "}");
                put("address_set_map", "{" +
                                       "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                                       "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                       "{'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                                       "}");
            }});
    private static Map<String, String> data5 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER5'");
                put("numbers", "[7,11,15]");
                put("number_set", "{7,11,15}");
                put("number_map", "{'i':1,'j':2}");
                put("address",
                    "[{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }]");
                put("address_set",
                    "{{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }}");
                put("address_map", "{'a': {city:'Granada',postcode:85964 }," +
                                   " 'b': {city:'Bilbao',postcode:270548 }," +
                                   " 'c': {city:'Sevilla',postcode:58964 }}");
                put("address_list_list",
                    "[[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                    "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                    "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]]");
                put("address_list_set",
                    "[{{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                    "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                    "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}]");
                put("address_list_map",
                    "[{'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                    "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                    "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}]");
                put("address_set_list",
                    "{[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                    "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                    "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]}");
                put("address_set_set",
                    "{{{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                    "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                    "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}}");
                put("address_set_map",
                    "{{'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                    "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                    "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}}");
            }});
    private static Map<String, String> data6 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER6'");
                put("numbers", "[4,10,15]");
                put("number_set", "{4,10,15}");
                put("number_map", "{'k':1,'d':2}");
                put("address",
                    "[{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }]");
                put("address_set",
                    "{{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }}");
                put("address_map", "{'a': {city:'Bilbao',postcode:270548 }," +
                                   " 'b': {city:'Venecia',postcode:28756 }," +
                                   " 'c': {city:'Barcelona',postcode:28059 }}");
                put("address_list_list", "[" +
                                         "[{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                                         "[{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                         "]");
                put("address_list_set", "[" +
                                        "{{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                        "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                        "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                        "]");
                put("address_list_map", "[" +
                                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                        "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                                        "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                        "]");
                put("address_set_list", "{" +
                                        "[{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                        "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                                        "[{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                        "}");
                put("address_set_set", "{" +
                                       "{{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                       "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                       "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                       "}");
                put("address_set_map", "{" +
                                       "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                       "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                                       "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                       "}");
            }});

    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME)
                                       .withTable("udt_collections_table")
                                       .withUDT("address_udt", "city", "text")
                                       .withUDT("address_udt", "postcode", "int")
                                       .withIndexColumn("lucene")
                                       .withColumn("login", "text")
                                       .withColumn("numbers", "list<int>", integerMapper())
                                       .withColumn("number_set", "set<int>", integerMapper())
                                       .withColumn("number_map", "map<text,int>", integerMapper())
                                       .withColumn("address", "list<frozen<address_udt>>")
                                       .withColumn("address_set", "set<frozen<address_udt>>")
                                       .withColumn("address_map", "map<text, frozen<address_udt>>")
                                       .withColumn("address_list_list", "list<frozen<list<address_udt>>>")
                                       .withColumn("address_list_set", "list<frozen<set<address_udt>>>")
                                       .withColumn("address_list_map", "list<frozen<map<text, address_udt>>>")
                                       .withColumn("address_set_list", "set<frozen<list<address_udt>>>")
                                       .withColumn("address_set_set", "set<frozen<set<address_udt>>>")
                                       .withColumn("address_set_map", "set<frozen<map<text, address_udt>>>")
                                       .withPartitionKey("login")
                                       .withMapper("address.city", stringMapper())
                                       .withMapper("address_set.city", stringMapper())
                                       .withMapper("address_map.city", stringMapper())
                                       .withMapper("address_list_list.city", stringMapper())
                                       .withMapper("address_list_set.city", stringMapper())
                                       .withMapper("address_list_map.city", stringMapper())
                                       .withMapper("address_set_list.city", stringMapper())
                                       .withMapper("address_set_set.city", stringMapper())
                                       .withMapper("address_set_map.city", stringMapper())
                                       .build()
                                       .createKeyspace()
                                       .createUDTs()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1, data2, data3, data4, data5, data6);
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testIntegerList() {
        cassandraUtils.filter(match("numbers", 1)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("numbers", 2)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("numbers", 3)).checkStringColumnWithoutOrder("login", "USER1", "USER4");
        cassandraUtils.filter(match("numbers", 4)).checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("numbers", 6)).checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("numbers", 7)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("numbers", 10))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("numbers", 11)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("numbers", 12)).checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("numbers", 14)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("numbers", 15)).checkStringColumnWithoutOrder("login", "USER5", "USER6");
        cassandraUtils.filter(match("numbers", 16)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("numbers", 17)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("numbers", 18)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("numbers", 20)).checkStringColumnWithoutOrder("login", "USER3", "USER4");
    }

    @Test
    public void testIntegerSet() {
        cassandraUtils.filter(match("number_set", 1)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("number_set", 2)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("number_set", 3)).checkStringColumnWithoutOrder("login", "USER1", "USER4");
        cassandraUtils.filter(match("number_set", 4)).checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("number_set", 6)).checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("number_set", 7)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("number_set", 10))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("number_set", 11)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("number_set", 12)).checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("number_set", 14)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("number_set", 15)).checkStringColumnWithoutOrder("login", "USER5", "USER6");
        cassandraUtils.filter(match("number_set", 16)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("number_set", 17)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("number_set", 18)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("number_set", 20)).checkStringColumnWithoutOrder("login", "USER3", "USER4");
    }

    @Test
    public void testTextIntegerMap() {
        cassandraUtils.filter(match("number_map$a", 1)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("number_map$b", 2)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("number_map$c", 1)).checkStringColumnWithoutOrder("login", "USER2", "USER4");
        cassandraUtils.filter(match("number_map$d", 2)).checkStringColumnWithoutOrder("login", "USER2", "USER6");
        cassandraUtils.filter(match("number_map$e", 1)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("number_map$f", 2)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("number_map$h", 2)).checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("number_map$i", 1)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("number_map$j", 2)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("number_map$k", 1)).checkStringColumnWithoutOrder("login", "USER6");
    }

    @Test
    public void testSimpleUDTList() {
        cassandraUtils.filter(match("address.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER6");
        cassandraUtils.filter(match("address.city", "Roma")).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address.city", "Valencia")).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address.city", "Bilbao"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address.city", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address.city", "Lisboa"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4");
        cassandraUtils.filter(match("address.city", "Sevilla"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER5");
        cassandraUtils.filter(match("address.city", "Granada"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5");
    }

    @Test
    public void testSimpleUDTSet() {
        cassandraUtils.filter(match("address_set.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER6");
        cassandraUtils.filter(match("address_set.city", "Roma")).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_set.city", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_set.city", "Bilbao"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set.city", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_set.city", "Lisboa"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4");
        cassandraUtils.filter(match("address_set.city", "Sevilla"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER5");
        cassandraUtils.filter(match("address_set.city", "Granada"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5");
    }

    @Test
    public void testSimpleUDTTextMap() {
        cassandraUtils.filter(match("address_map.city$a", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_map.city$b", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address_map.city$c", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("address_map.city$a", "Roma")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$b", "Roma")).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_map.city$c", "Roma")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$a", "Valencia")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$b", "Valencia")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$c", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_map.city$a", "Bilbao"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER6");
        cassandraUtils.filter(match("address_map.city$b", "Bilbao"))
                      .checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address_map.city$c", "Bilbao")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$a", "Venecia")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$b", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_map.city$c", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address_map.city$a", "Lisboa"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_map.city$b", "Lisboa")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$c", "Lisboa"))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address_map.city$a", "Sevilla")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$b", "Sevilla"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_map.city$c", "Sevilla"))
                      .checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address_map.city$a", "Granada"))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER5");
        cassandraUtils.filter(match("address_map.city$b", "Granada")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_map.city$c", "Granada"))
                      .checkStringColumnWithoutOrder("login", "USER3");
    }

    @Test
    public void testSimpleUDTListOfList() {
        cassandraUtils.filter(match("address_list_list.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_list.city", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Bilbao")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Venecia")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Lisboa")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Sevilla")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Granada")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_list.city", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Malaga")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Castellon")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_list.city", "Tarragona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER4");
    }

    @Test
    public void testSimpleUDTListOfSet() {
        cassandraUtils.filter(match("address_list_set.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_list_set.city", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_set.city", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Bilbao")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_list_set.city", "Lisboa")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Sevilla")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3", "USER5");
        cassandraUtils.filter(match("address_list_set.city", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Malaga")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Castellon"))
                      .checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Tarragona"))
                      .checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "Burgos")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_list_set.city", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER5", "USER6");
    }

    @Test
    public void testSimpleUDTListOfMapMap() {
        cassandraUtils.filter(match("address_list_map.city$a", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$b", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_map.city$f", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_list_map.city$g", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_map.city$c", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$d", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_map.city$e", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$h", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Toledo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$c", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_list_map.city$e", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3");
        cassandraUtils.filter(match("address_list_map.city$f", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER5");
        cassandraUtils.filter(match("address_list_map.city$g", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_list_map.city$b", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address_list_map.city$c", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address_list_map.city$d", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_list_map.city$f", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("address_list_map.city$h", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address_list_map.city$c", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address_list_map.city$d", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER4");
    }

    @Test
    public void testSimpleUDTSetOfList() {
        cassandraUtils.filter(match("address_set_list.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_list.city", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Bilbao")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Venecia")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Lisboa")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Sevilla")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Granada")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_list.city", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Malaga")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Castellon")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_list.city", "Tarragona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER4");
    }

    @Test
    public void testSimpleUDTSetOfSet() {
        cassandraUtils.filter(match("address_set_set.city", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_set_set.city", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_set.city", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Bilbao")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Venecia"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_set_set.city", "Lisboa")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Sevilla")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3", "USER5");
        cassandraUtils.filter(match("address_set_set.city", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Malaga")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Castellon")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Tarragona")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "Burgos")).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address_set_set.city", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER5", "USER6");
    }

    @Test
    public void testSimpleUDTSetOfMapMap() {
        cassandraUtils.filter(match("address_set_map.city$a", "Barcelona"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$b", "Roma"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_map.city$f", "Jaen"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_set_map.city$g", "Aviles"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_map.city$c", "Valencia"))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$d", "Oviedo"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_map.city$e", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$h", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Toledo"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$c", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address_set_map.city$e", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3");
        cassandraUtils.filter(match("address_set_map.city$f", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER5");
        cassandraUtils.filter(match("address_set_map.city$g", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Salamanca"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_set_map.city$b", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address_set_map.city$c", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address_set_map.city$d", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address_set_map.city$f", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("address_set_map.city$h", "San Sebastian"))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address_set_map.city$c", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address_set_map.city$d", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Valladolid"))
                      .checkStringColumnWithoutOrder("login", "USER4");
    }
}
