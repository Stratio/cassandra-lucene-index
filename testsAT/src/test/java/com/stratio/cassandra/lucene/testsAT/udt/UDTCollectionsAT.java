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
    static final String KEYSPACE_NAME = "udt_collections";
    private static String[] datakeys = {};
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
                                       .insert(data1, data2, data3, data4, data5, data6)
                                       .refresh();
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testIntegerList() {
        cassandraUtils.filter(match("numbers", 1)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("numbers", 2)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("numbers", 3)).checkUnorderedStringColumns("login", "USER1", "USER4");
        cassandraUtils.filter(match("numbers", 4)).checkStringColumn("login", "USER6");
        cassandraUtils.filter(match("numbers", 6)).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("numbers", 7)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("numbers", 10)).checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("numbers", 11)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("numbers", 12)).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("numbers", 14)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("numbers", 15)).checkUnorderedStringColumns("login", "USER5", "USER6");
        cassandraUtils.filter(match("numbers", 16)).check(0);
        cassandraUtils.filter(match("numbers", 17)).check(0);
        cassandraUtils.filter(match("numbers", 18)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("numbers", 20)).checkUnorderedStringColumns("login", "USER3", "USER4");
    }

    @Test
    public void testIntegerSet() {
        cassandraUtils.filter(match("number_set", 1)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("number_set", 2)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("number_set", 3)).checkUnorderedStringColumns("login", "USER1", "USER4");
        cassandraUtils.filter(match("number_set", 4)).checkStringColumn("login", "USER6");
        cassandraUtils.filter(match("number_set", 6)).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("number_set", 7)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("number_set", 10))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("number_set", 11)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("number_set", 12)).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("number_set", 14)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("number_set", 15)).checkUnorderedStringColumns("login", "USER5", "USER6");
        cassandraUtils.filter(match("number_set", 16)).check(0);
        cassandraUtils.filter(match("number_set", 17)).check(0);
        cassandraUtils.filter(match("number_set", 18)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("number_set", 20)).checkUnorderedStringColumns("login", "USER3", "USER4");
    }

    @Test
    public void testTextIntegerMap() {
        cassandraUtils.filter(match("number_map$a", 1)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("number_map$b", 2)).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("number_map$c", 1)).checkUnorderedStringColumns("login", "USER2", "USER4");
        cassandraUtils.filter(match("number_map$d", 2)).checkUnorderedStringColumns("login", "USER2", "USER6");
        cassandraUtils.filter(match("number_map$e", 1)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("number_map$f", 2)).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("number_map$h", 2)).checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("number_map$i", 1)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("number_map$j", 2)).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("number_map$k", 1)).checkStringColumn("login", "USER6");
    }

    @Test
    public void testSimpleUDTList() {
        cassandraUtils.filter(match("address.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER6");
        cassandraUtils.filter(match("address.city", "Roma")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address.city", "Valencia")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address.city", "Bilbao"))
                      .checkUnorderedStringColumns("login", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address.city", "Venecia"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address.city", "Lisboa")).checkUnorderedStringColumns("login", "USER3", "USER4");
        cassandraUtils.filter(match("address.city", "Sevilla"))
                      .checkUnorderedStringColumns("login", "USER3", "USER5");
        cassandraUtils.filter(match("address.city", "Granada"))
                      .checkUnorderedStringColumns("login", "USER3", "USER4", "USER5");
    }

    @Test
    public void testSimpleUDTSet() {
        cassandraUtils.filter(match("address_set.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER6");
        cassandraUtils.filter(match("address_set.city", "Roma")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_set.city", "Valencia")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_set.city", "Bilbao"))
                      .checkUnorderedStringColumns("login", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set.city", "Venecia"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_set.city", "Lisboa"))
                      .checkUnorderedStringColumns("login", "USER3", "USER4");
        cassandraUtils.filter(match("address_set.city", "Sevilla"))
                      .checkUnorderedStringColumns("login", "USER3", "USER5");
        cassandraUtils.filter(match("address_set.city", "Granada"))
                      .checkUnorderedStringColumns("login", "USER3", "USER4", "USER5");
    }

    @Test
    public void testSimpleUDTTextMap() {
        cassandraUtils.filter(match("address_map.city$a", "Barcelona")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_map.city$b", "Barcelona")).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("address_map.city$c", "Barcelona")).checkStringColumn("login", "USER6");
        cassandraUtils.filter(match("address_map.city$a", "Roma")).check(0);
        cassandraUtils.filter(match("address_map.city$b", "Roma")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_map.city$c", "Roma")).check(0);
        cassandraUtils.filter(match("address_map.city$a", "Valencia")).check(0);
        cassandraUtils.filter(match("address_map.city$b", "Valencia")).check(0);
        cassandraUtils.filter(match("address_map.city$c", "Valencia")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_map.city$a", "Bilbao"))
                      .checkUnorderedStringColumns("login", "USER2", "USER6");
        cassandraUtils.filter(match("address_map.city$b", "Bilbao")).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("address_map.city$c", "Bilbao")).check(0);
        cassandraUtils.filter(match("address_map.city$a", "Venecia")).check(0);
        cassandraUtils.filter(match("address_map.city$b", "Venecia"))
                      .checkUnorderedStringColumns("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_map.city$c", "Venecia")).checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("address_map.city$a", "Lisboa")).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_map.city$b", "Lisboa")).check(0);
        cassandraUtils.filter(match("address_map.city$c", "Lisboa")).checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("address_map.city$a", "Sevilla")).check(0);
        cassandraUtils.filter(match("address_map.city$b", "Sevilla")).checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_map.city$c", "Sevilla")).checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("address_map.city$a", "Granada"))
                      .checkUnorderedStringColumns("login", "USER4", "USER5");
        cassandraUtils.filter(match("address_map.city$b", "Granada")).check(0);
        cassandraUtils.filter(match("address_map.city$c", "Granada")).checkStringColumn("login", "USER3");
    }

    @Test
    public void testSimpleUDTListOfList() {
        cassandraUtils.filter(match("address_list_list.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_list.city", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Bilbao")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Venecia")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Lisboa")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Sevilla")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Granada")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_list.city", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Malaga")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Castellon")).check(0);
        cassandraUtils.filter(match("address_list_list.city", "Tarragona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "San Sebastian"))
                      .checkUnorderedStringColumns("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_list.city", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER4");
    }

    @Test
    public void testSimpleUDTListOfSet() {
        cassandraUtils.filter(match("address_list_set.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_list_set.city", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_set.city", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Bilbao")).check(0);
        cassandraUtils.filter(match("address_list_set.city", "Venecia"))
                      .checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_list_set.city", "Lisboa")).check(0);
        cassandraUtils.filter(match("address_list_set.city", "Sevilla")).check(0);
        cassandraUtils.filter(match("address_list_set.city", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER1", "USER3", "USER5");
        cassandraUtils.filter(match("address_list_set.city", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Malaga")).check(0);
        cassandraUtils.filter(match("address_list_set.city", "Castellon"))
                      .check(0);
        cassandraUtils.filter(match("address_list_set.city", "Tarragona"))
                      .check(0);
        cassandraUtils.filter(match("address_list_set.city", "Burgos")).check(0);
        cassandraUtils.filter(match("address_list_set.city", "San Sebastian"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_set.city", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER5", "USER6");
    }

    @Test
    public void testSimpleUDTListOfMapMap() {
        cassandraUtils.filter(match("address_list_map.city$a", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$b", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_list_map.city$f", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_list_map.city$g", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_map.city$c", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$d", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER5");
        cassandraUtils.filter(match("address_list_map.city$e", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$h", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Toledo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_list_map.city$c", "Salamanca"))
                      .checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_list_map.city$e", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER3");
        cassandraUtils.filter(match("address_list_map.city$f", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER5");
        cassandraUtils.filter(match("address_list_map.city$g", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Salamanca"))
                      .checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_list_map.city$b", "San Sebastian"))
                      .checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("address_list_map.city$c", "San Sebastian"))
                      .checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("address_list_map.city$d", "San Sebastian"))
                      .checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_list_map.city$f", "San Sebastian"))
                      .checkStringColumn("login", "USER6");
        cassandraUtils.filter(match("address_list_map.city$h", "San Sebastian"))
                      .checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("address_list_map.city$c", "Valladolid"))
                      .checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("address_list_map.city$d", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_list_map.city$i", "Valladolid"))
                      .checkStringColumn("login", "USER4");
    }

    @Test
    public void testSimpleUDTSetOfList() {
        cassandraUtils.filter(match("address_set_list.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_list.city", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Bilbao")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Venecia")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Lisboa")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Sevilla")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Granada")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_list.city", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Malaga")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Castellon")).check(0);
        cassandraUtils.filter(match("address_set_list.city", "Tarragona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "San Sebastian"))
                      .checkUnorderedStringColumns("login", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_list.city", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER4");
    }

    @Test
    public void testSimpleUDTSetOfSet() {
        cassandraUtils.filter(match("address_set_set.city", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_set_set.city", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_set.city", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Bilbao")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Venecia")).checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_set_set.city", "Lisboa")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Sevilla")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER1", "USER3", "USER5");
        cassandraUtils.filter(match("address_set_set.city", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Malaga")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Castellon")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Tarragona")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "Burgos")).check(0);
        cassandraUtils.filter(match("address_set_set.city", "San Sebastian"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_set.city", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER5", "USER6");
    }

    @Test
    public void testSimpleUDTSetOfMapMap() {
        cassandraUtils.filter(match("address_set_map.city$a", "Barcelona"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$b", "Roma"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER4", "USER6");
        cassandraUtils.filter(match("address_set_map.city$f", "Jaen"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address_set_map.city$g", "Aviles"))
                      .checkUnorderedStringColumns("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_map.city$c", "Valencia"))
                      .checkUnorderedStringColumns("login", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$d", "Oviedo"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER5");
        cassandraUtils.filter(match("address_set_map.city$e", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER2", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$h", "Madrid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER3", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Toledo"))
                      .checkUnorderedStringColumns("login", "USER1", "USER2", "USER5", "USER6");
        cassandraUtils.filter(match("address_set_map.city$c", "Salamanca"))
                      .checkStringColumn("login", "USER1");
        cassandraUtils.filter(match("address_set_map.city$e", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER3");
        cassandraUtils.filter(match("address_set_map.city$f", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER5");
        cassandraUtils.filter(match("address_set_map.city$g", "Salamanca"))
                      .checkUnorderedStringColumns("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Salamanca"))
                      .checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_set_map.city$b", "San Sebastian"))
                      .checkStringColumn("login", "USER5");
        cassandraUtils.filter(match("address_set_map.city$c", "San Sebastian"))
                      .checkStringColumn("login", "USER2");
        cassandraUtils.filter(match("address_set_map.city$d", "San Sebastian"))
                      .checkStringColumn("login", "USER3");
        cassandraUtils.filter(match("address_set_map.city$f", "San Sebastian"))
                      .checkStringColumn("login", "USER6");
        cassandraUtils.filter(match("address_set_map.city$h", "San Sebastian"))
                      .checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("address_set_map.city$c", "Valladolid"))
                      .checkStringColumn("login", "USER4");
        cassandraUtils.filter(match("address_set_map.city$d", "Valladolid"))
                      .checkUnorderedStringColumns("login", "USER1", "USER6");
        cassandraUtils.filter(match("address_set_map.city$i", "Valladolid"))
                      .checkStringColumn("login", "USER4");
    }
}
