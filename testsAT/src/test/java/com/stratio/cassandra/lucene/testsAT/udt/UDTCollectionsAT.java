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

package com.stratio.cassandra.lucene.testsAT.udt;

import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */

@RunWith(JUnit4.class)
public class UDTCollectionsAT extends BaseAT {

    private static CassandraUtils cassandraUtils;

    static final String KEYSPACE_NAME = "udt_collections";

    @BeforeClass
    public static void before() {
        Mapper stringMapper = stringMapper();
        Mapper integerMapper = integerMapper();

        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME)
                                       .withUDT("address_udt", "city", "text")
                                       .withUDT("address_udt", "postcode", "int")
                                       .withColumn("login", "text")
                                       .withColumn("numbers", "list<int>", integerMapper)
                                       .withColumn("number_set", "set<int>", integerMapper)
                                       .withColumn("number_map", "map<text,int>", integerMapper)
                                       .withColumn("address", "list<frozen<address_udt>>")
                                       .withColumn("address_set", "set<frozen<address_udt>>")
                                       .withColumn("address_map", "map<text,frozen<address_udt>>")
                                       .withColumn("address_list_list", "list<frozen<list<address_udt>>>")
                                       .withColumn("address_list_set", "list<frozen<set<address_udt>>>")
                                       .withColumn("address_list_map", "list<frozen<map<text,address_udt>>>")
                                       .withColumn("address_set_list", "set<frozen<list<address_udt>>>")
                                       .withColumn("address_set_set", "set<frozen<set<address_udt>>>")
                                       .withColumn("address_set_map", "set<frozen<map<text,address_udt>>>")
                                       .withColumn("lucene", "text")
                                       .withPartitionKey("login")
                                       .withMapper("address.city", stringMapper)
                                       .withMapper("address_set.city", stringMapper)
                                       .withMapper("address_map.city", stringMapper)
                                       .withMapper("address_list_list.city", stringMapper)
                                       .withMapper("address_list_set.city", stringMapper)
                                       .withMapper("address_list_map.city", stringMapper)
                                       .withMapper("address_set_list.city", stringMapper)
                                       .withMapper("address_set_set.city", stringMapper)
                                       .withMapper("address_set_map.city", stringMapper)
                                       .build()
                                       .createKeyspace()
                                       .createUDTs()
                                       .createTable()
                                       .createIndex();

        Map<String, String> data = new HashMap<>();
        data.put("login", "'USER1'");
        data.put("numbers", "[1,2,3]");
        data.put("number_set", "{1,2,3}");
        data.put("number_map", "{'a': 1, 'b': 2 }");
        data.put("address",
                 "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]");
        data.put("address_set",
                 "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964}}");
        data.put("address_map",
                 "{'a': {city:'Barcelona',postcode:28059 }, 'b': {city:'Roma',postcode: 29506},  'c': {city:'Valencia',postcode:85964 }}");
        data.put("address_list_list",
                 "[[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                 " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                 " [{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]]");
        data.put("address_list_set",
                 "[{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                 " {{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                 " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}]");
        data.put("address_list_map",
                 "[{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                 " {'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                 " {'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}]");
        data.put("address_set_list",
                 "{[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                 " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                 " [{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]}");
        data.put("address_set_set",
                 "{{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                 " {{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                 " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}}");
        data.put("address_set_map",
                 "{{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                 " {'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                 " {'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}}");

        Map<String, String> data2 = new HashMap<>();
        data2.put("login", "'USER2'");
        data2.put("numbers", "[6,10,12]");
        data2.put("number_set", "{6,10,12}");
        data2.put("number_map", "{'c':1,'d':2}");
        data2.put("address",
                  "[{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }]");
        data2.put("address_set",
                  "{{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }}");
        data2.put("address_map", "{'a': {city:'Bilbao',postcode:270548 }," +
                                 " 'b': {city:'Barcelona',postcode:28059 }," +
                                 " 'c': {city:'Venecia',postcode:28756 }}");
        data2.put("address_list_list",
                  "[[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                  " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                  " [{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]]");
        data2.put("address_list_set",
                  "[{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                  " {{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                  " {{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}]");
        data2.put("address_list_map",
                  "[{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                  " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                  " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}]");
        data2.put("address_set_list",
                  "{[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                  " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                  " [{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]}");
        data2.put("address_set_set",
                  "{{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                  " {{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                  " {{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}}");
        data2.put("address_set_map",
                  "{{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                  " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                  " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}}");

        Map<String, String> data3 = new HashMap<>();
        data3.put("login", "'USER3'");
        data3.put("numbers", "[14,18,20]");
        data3.put("number_set", "{14,18,20}");
        data3.put("number_map", "{'e':1,'f':2}");
        data3.put("address",
                  "[{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }]");
        data3.put("address_set",
                  "{{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }}");
        data3.put("address_map", "{'a': {city:'Lisboa',postcode:29685 }," +
                                 " 'b': {city:'Sevilla',postcode:58964 }," +
                                 " 'c': {city:'Granada',postcode:85964 }}");
        data3.put("address_list_list",
                  "[[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                  " [{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                  " [{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]]");
        data3.put("address_list_set",
                  "[{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                  " {{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                  " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}]");
        data3.put("address_list_map",
                  "[{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                  " {'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                  " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}]");
        data3.put("address_set_list",
                  "{[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                  " [{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                  " [{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]}");
        data3.put("address_set_set",
                  "{{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                  " {{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                  " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}}");
        data3.put("address_set_map",
                  "{{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                  " {'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                  " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}}");

        Map<String, String> data4 = new HashMap<>();
        data4.put("login", "'USER4'");
        data4.put("numbers", "[3,10,20]");
        data4.put("number_set", "{3,10,20}");
        data4.put("number_map", "{'c':1,'h':2}");
        data4.put("address",
                  "[{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }]");
        data4.put("address_set",
                  "{{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }}");
        data4.put("address_map", "{'a': {city:'Granada',postcode:85964 }," +
                                 " 'b': {city:'Venecia',postcode:28756 }," +
                                 " 'c': {city:'Lisboa',postcode:29685 }}");
        data4.put("address_list_list", "[" +
                                       " [{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                                       " [{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                       " [{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                       "]");
        data4.put("address_list_set", "[" +
                                      " {{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                      " {{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                      " {{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                      "]");
        data4.put("address_list_map", "[" +
                                      " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                                      " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                      " {'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                                      "]");
        data4.put("address_set_list", "{" +
                                      " [{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                                      " [{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                      " [{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                      "}");
        data4.put("address_set_set", "{" +
                                     " {{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                     " {{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                     " {{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                     "}");
        data4.put("address_set_map", "{" +
                                     " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                                     " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                                     " {'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                                     "}");

        Map<String, String> data5 = new HashMap<>();
        data5.put("login", "'USER5'");
        data5.put("numbers", "[7,11,15]");
        data5.put("number_set", "{7,11,15}");
        data5.put("number_map", "{'i':1,'j':2}");
        data5.put("address",
                  "[{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }]");
        data5.put("address_set",
                  "{{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }}");
        data5.put("address_map", "{'a': {city:'Granada',postcode:85964 }," +
                                 " 'b': {city:'Bilbao',postcode:270548 }," +
                                 " 'c': {city:'Sevilla',postcode:58964 }}");
        data5.put("address_list_list", "[" +
                                       " [{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                       " [{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                       " [{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                       "]");
        data5.put("address_list_set", "[" +
                                      " {{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                      " {{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                      " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                      "]");
        data5.put("address_list_map", "[" +
                                      " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                      " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                                      " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                      "]");
        data5.put("address_set_list", "{" +
                                      " [{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                      " [{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                                      " [{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                      "}");
        data5.put("address_set_set", "{" +
                                     " {{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                     " {{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                     " {{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                     "}");
        data5.put("address_set_map", "{" +
                                     " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                     " {'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                                     " {'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                     "}");

        Map<String, String> data6 = new HashMap<>();
        data6.put("login", "'USER6'");
        data6.put("numbers", "[4,10,15]");
        data6.put("number_set", "{4,10,15}");
        data6.put("number_map", "{'k':1,'d':2}");
        data6.put("address",
                  "[{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }]");
        data6.put("address_set",
                  "{{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }}");
        data6.put("address_map", "{'a': {city:'Bilbao',postcode:270548 }," +
                                 " 'b': {city:'Venecia',postcode:28756 }," +
                                 " 'c': {city:'Barcelona',postcode:28059 }}");
        data6.put("address_list_list", "[" +
                                       " [{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                       " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                                       " [{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                       "]");
        data6.put("address_list_set", "[" +
                                      " {{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                      " {{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                      " {{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                      "]");
        data6.put("address_list_map", "[" +
                                      " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                      " {'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                                      " {'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                      "]");
        data6.put("address_set_list", "{" +
                                      " [{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                                      " [{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                                      " [{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                                      "}");
        data6.put("address_set_set", "{" +
                                     " {{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                                     " {{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                                     " {{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                                     "}");
        data6.put("address_set_map", "{" +
                                     " {'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                                     " {'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                                     " {'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                                     "}");

        cassandraUtils.insert(data, data2, data3, data4, data5, data6);
        cassandraUtils.refresh();

    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    private boolean isThisAndOnlyThis(String[] received, String[] expected) {
        if (received.length != expected.length) {
            return false;
        } else {

            for (String i : received) {
                boolean found = false;
                for (String j : expected) {
                    if (i.equals(j)) {
                        found = true;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
    }

    private void assertEqualsAndOnlyThisString(String[] received, String[] expected) {
        assertEquals("Expected " + expected.length + " results but received: " + received.length,
                     expected.length,
                     received.length);
        assertTrue("Unexpected results!! Expected: " + Arrays.toString(expected) + ", but got: " +
                   Arrays.toString(received), isThisAndOnlyThis(received, expected));

    }

    @Test
    public void testIntegerList() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("numbers", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("numbers", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("numbers", 3));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4"});

        select = cassandraUtils.filter(match("numbers", 4));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("numbers", 6));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("numbers", 7));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("numbers", 10));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("numbers", 11));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("numbers", 12));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("numbers", 14));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("numbers", 15));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5", "USER6"});

        select = cassandraUtils.filter(match("numbers", 16));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("numbers", 17));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("numbers", 18));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("numbers", 20));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4"});

    }

    @Test
    public void testIntegerSet() {
        CassandraUtilsSelect select = cassandraUtils.filter(match("number_set", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("number_set", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("number_set", 3));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4"});

        select = cassandraUtils.filter(match("number_set", 4));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("number_set", 6));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("number_set", 7));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("number_set", 10));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("number_set", 11));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("number_set", 12));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("number_set", 14));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("number_set", 15));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5", "USER6"});

        select = cassandraUtils.filter(match("number_set", 16));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("number_set", 17));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("number_set", 18));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("number_set", 20));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4"});

    }

    @Test
    public void testTextIntegerMap() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("number_map$a", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("number_map$b", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("number_map$c", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4"});

        select = cassandraUtils.filter(match("number_map$d", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER6"});

        select = cassandraUtils.filter(match("number_map$e", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("number_map$f", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("number_map$h", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("number_map$i", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("number_map$j", 2));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("number_map$k", 1));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

    }

    @Test
    public void testSimpleUDTList() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("address.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER6"});

        select = cassandraUtils.filter(match("address.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4"});

        select = cassandraUtils.filter(match("address.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5"});

        select = cassandraUtils.filter(match("address.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4", "USER5"});

    }

    @Test
    public void testSimpleUDTSet() {
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_set.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER6"});

        select = cassandraUtils.filter(match("address_set.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_set.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_set.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4"});

        select = cassandraUtils.filter(match("address_set.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5"});

        select = cassandraUtils.filter(match("address_set.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4", "USER5"});

    }

    @Test
    public void testSimpleUDTTextMap() {
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_map.city$a", "Barcelona"));

        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map.city$b", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_map.city$c", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_map.city$a", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$b", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map.city$c", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$a", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$b", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$c", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map.city$a", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER6"});

        select = cassandraUtils.filter(match("address_map.city$b", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_map.city$c", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$a", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$b", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER6"});

        select = cassandraUtils.filter(match("address_map.city$c", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_map.city$a", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3",});

        select = cassandraUtils.filter(match("address_map.city$b", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$c", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_map.city$a", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$b", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_map.city$c", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_map.city$a", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER5"});

        select = cassandraUtils.filter(match("address_map.city$b", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map.city$c", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

    }

    @Test
    public void testSimpleUDTListOfList() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("address_list_list.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_list.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_list.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Malaga"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Castellon"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_list.city", "Tarragona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_list.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4"});

    }

    @Test
    public void testSimpleUDTListOfSet() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("address_list_set.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_list_set.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_set.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_list_set.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3", "USER5"});

        select = cassandraUtils.filter(match("address_list_set.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Malaga"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Castellon"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Tarragona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "Burgos"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_list_set.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_set.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER5", "USER6"});
    }

    @Test
    public void testSimpleUDTListOfMapMap() {
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_list_map.city$a", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$b", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$f", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_list_map.city$g", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_map.city$c", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$d", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_map.city$e", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$h", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$i", "Toledo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$c", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_list_map.city$e", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3"});

        select = cassandraUtils.filter(match("address_list_map.city$f", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER5"});

        select = cassandraUtils.filter(match("address_list_map.city$g", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$i", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_list_map.city$b", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_list_map.city$c", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_list_map.city$d", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_list_map.city$f", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$h", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_list_map.city$c", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_list_map.city$d", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_list_map.city$i", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});
    }

    @Test
    public void testSimpleUDTSetOfList() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("address_set_list.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_list.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_list.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Malaga"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Castellon"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_list.city", "Tarragona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_list.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4"});

    }

    @Test
    public void testSimpleUDTSetOfSet() {

        CassandraUtilsSelect select = cassandraUtils.filter(match("address_set_set.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_set_set.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_set.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_set_set.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3", "USER5"});

        select = cassandraUtils.filter(match("address_set_set.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Malaga"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Castellon"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Tarragona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "Burgos"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_set_set.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_set.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER5", "USER6"});
    }

    @Test
    public void testSimpleUDTSetOfMapMap() {
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_set_map.city$a", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$b", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$f", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_set_map.city$g", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_map.city$c", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$d", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_map.city$e", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$h", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$i", "Toledo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$c", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_set_map.city$e", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3"});

        select = cassandraUtils.filter(match("address_set_map.city$f", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER5"});

        select = cassandraUtils.filter(match("address_set_map.city$g", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$i", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_set_map.city$b", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_set_map.city$c", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_set_map.city$d", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_set_map.city$f", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$h", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_set_map.city$c", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_set_map.city$d", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_set_map.city$i", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});
    }
}
