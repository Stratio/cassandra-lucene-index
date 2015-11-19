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

import com.datastax.driver.core.SimpleStatement;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import com.stratio.cassandra.lucene.testsAT.util.UDT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

import static com.stratio.cassandra.lucene.builder.Builder.match;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */

@RunWith(JUnit4.class)
public class UDTCollectionsTest {

    private static CassandraUtils cassandraUtils;

    static final String KEYSPACE_NAME = "udt_collections";

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city", "text");
        addressUDT.add("postcode", "int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " numbers list<int>, number_set set<int>,number_map map<text,int>," +
                                    " address list<frozen<address_udt>>, address_set set<frozen<address_udt>>, address_map map<text,frozen<address_udt>>," +
                                    " address_list_list list<frozen<list<address_udt>>>, address_list_set list<frozen<set<address_udt>>>, address_list_map list<frozen<map<text,address_udt>>>, " +
                                    " address_set_list set<frozen<list<address_udt>>>, address_set_set set<frozen<set<address_udt>>>, address_set_map set<frozen<map<text,address_udt>>>, " +
                                    "  lucene text);";

        cassandraUtils.execute(new SimpleStatement(useKeyspaceQuery));
        cassandraUtils.execute(new SimpleStatement(addressUDT.build()));
        cassandraUtils.execute(new SimpleStatement(tableCreationQuery));

        String createIndexQuery = "CREATE CUSTOM INDEX test_index ON " +
                                  cassandraUtils.getKeyspace() +
                                  "." +
                                  cassandraUtils.getTable() +
                                  "(lucene) " +
                                  "USING 'com.stratio.cassandra.lucene.Index' " +
                                  "WITH OPTIONS = { " +
                                  "'refresh_seconds' : '1'," +
                                  "'schema' : '{" +
                                  "fields : { " +
                                  "numbers : {type:\"integer\"}," +
                                  "number_set : {type:\"integer\"}," +
                                  "number_map : {type:\"integer\"}," +
                                  "\"address.city\" : {type:\"string\"}," +
                                  "\"address_set.city\" : {type:\"string\"}," +
                                  "\"address_map.city\" : {type:\"string\"}," +
                                  "\"address_list_list.city\" : {type:\"string\"}," +
                                  "\"address_list_set.city\" : {type:\"string\"}," +
                                  "\"address_list_map.city\" : {type:\"string\"}," +
                                  "\"address_set_list.city\" : {type:\"string\"}," +
                                  "\"address_set_set.city\" : {type:\"string\"}," +
                                  "\"address_set_map.city\" : {type:\"string\"}" +
                                  "}}'};";

        cassandraUtils.execute(new SimpleStatement(createIndexQuery));

        String insert = "INSERT INTO " +
                        cassandraUtils.getKeyspace() +
                        "." +
                        cassandraUtils.getTable() +
                        "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map,address_set_list, address_set_set, address_set_map) " +
                        "VALUES ('USER1',[1,2,3],{1,2,3},{'a': 1, 'b': 2 }, " +
                        "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964}}," +
                        "{'a': {city:'Barcelona',postcode:28059 }," +
                        " 'b': {city:'Roma',postcode: 29506}," +
                        " 'c': {city:'Valencia',postcode:85964 }}," +
                        "[" +
                        "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                        "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                        "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                        "], " +
                        "[" +
                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                        "{{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                        "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                        "], " +
                        "[" +
                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                        "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                        "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                        "]," +
                        "{" +
                        "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                        "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                        "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                        "}, " +
                        "{" +
                        "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                        "{{city:'Oviedo',postcode:28059 },{city:'Venecia',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                        "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                        "}, " +
                        "{" +
                        "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Salamanca',postcode: 85964 }}," +
                        "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                        "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                        "});";

        String insert2 = "INSERT INTO " +
                         cassandraUtils.getKeyspace() +
                         "." +
                         cassandraUtils.getTable() +
                         "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map ,address_set_list, address_set_set, address_set_map)" +
                         "VALUES ('USER2',[6,10,12],{6,10,12},{'c':1,'d':2}," +
                         "[{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }]," +
                         "{{city:'Bilbao',postcode:270548 },{city:'Barcelona',postcode:28059 },{city:'Venecia',postcode: 28756 }}," +
                         "{'a': {city:'Bilbao',postcode:270548 }," +
                         " 'b': {city:'Barcelona',postcode:28059 }," +
                         " 'c': {city:'Venecia',postcode:28756 }}," +
                         "[" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]" +
                         "], " +
                         "[" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "], " +
                         "[" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "]," +
                         "{" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Tarragona',postcode: 85964 }]" +
                         "}, " +
                         "{" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Valladolid',postcode:28059 },{city:'San Sebastian',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "}, " +
                         "{" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'San Sebastian',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "});";

        String insert3 = "INSERT INTO " +
                         cassandraUtils.getKeyspace() +
                         "." +
                         cassandraUtils.getTable() +
                         "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map,address_set_list, address_set_set, address_set_map )" +
                         "VALUES ('USER3',[14,18,20],{14,18,20},{'e':1,'f':2}," +
                         "[{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }]," +
                         "{{city:'Lisboa',postcode:29685 },{city:'Sevilla',postcode:58964 },{city:'Granada',postcode:85964 }}," +
                         "{'a': {city:'Lisboa',postcode:29685 }," +
                         " 'b': {city:'Sevilla',postcode:58964 }," +
                         " 'c': {city:'Granada',postcode:85964 }}," +
                         "[" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "], " +
                         "[" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "], " +
                         "[" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}" +
                         "]," +
                         "{" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Tarragona',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "}, " +
                         "{" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Valladolid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "}, " +
                         "{" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'San Sebastian',postcode:28059 },'e': {city:'Salamanca',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Salamanca',postcode: 85964 }}" +
                         "});";

        String insert4 = "INSERT INTO " +
                         cassandraUtils.getKeyspace() +
                         "." +
                         cassandraUtils.getTable() +
                         "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map ,address_set_list, address_set_set, address_set_map) " +
                         "VALUES ('USER4',[3,10,20],{3,10,20},{'c':1,'h':2}," +
                         "[{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }]," +
                         "{{city:'Granada',postcode:85964 },{city:'Venecia',postcode:28756 },{city:'Lisboa',postcode:29685 }}," +
                         "{'a': {city:'Granada',postcode:85964 }," +
                         " 'b': {city:'Venecia',postcode:28756 }," +
                         " 'c': {city:'Lisboa',postcode:29685 }}," +
                         "[" +
                         "[{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                         "[{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "], " +
                         "[" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "], " +
                         "[" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                         "]," +
                         "{" +
                         "[{city:'Salamanca',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valladolid',postcode: 85964 }]," +
                         "[{city:'San Sebastian',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "}, " +
                         "{" +
                         "{{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "}, " +
                         "{" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valladolid',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Jaen',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'San Sebastian',postcode:29506 },'i': {city:'Valladolid',postcode: 85964 }}" +
                         "}" +
                         ");";

        String insert5 = "INSERT INTO " +
                         cassandraUtils.getKeyspace() +
                         "." +
                         cassandraUtils.getTable() +
                         "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map ,address_set_list, address_set_set, address_set_map) " +
                         "VALUES ('USER5',[7,11,15],{7,11,15},{'i':1,'j':2}," +
                         "[{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }]," +
                         "{{city:'Granada',postcode:85964 },{city:'Bilbao',postcode:270548 },{city:'Sevilla',postcode:58964 }}," +
                         "{'a': {city:'Granada',postcode:85964 }," +
                         " 'b': {city:'Bilbao',postcode:270548 }," +
                         " 'c': {city:'Sevilla',postcode:58964 }}," +
                         "[" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "], " +
                         "[" +
                         "{{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "], " +
                         "[" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "]," +
                         "{" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Jaen',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "}, " +
                         "{" +
                         "{{city:'Valladolid',postcode:28059 },{city:'Roma',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Salamanca',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'Aviles',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "}, " +
                         "{" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'San Sebastian',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'Oviedo',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'Salamanca',postcode: 85964 }}," +
                         "{'g': {city:'Aviles',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "});";

        String insert6 = "INSERT INTO " +
                         cassandraUtils.getKeyspace() +
                         "." +
                         cassandraUtils.getTable() +
                         "(login, numbers, number_set, number_map, address, address_set, address_map, address_list_list, address_list_set, address_list_map ,address_set_list, address_set_set, address_set_map) " +
                         "VALUES ('USER6',[4,10,15],{4,10,15},{'k':1,'d':2}," +
                         "[{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }]," +
                         "{{city:'Bilbao',postcode:270548 },{city:'Venecia',postcode:28756 },{city:'Barcelona',postcode:28059 }}," +
                         "{'a': {city:'Bilbao',postcode:270548 }," +
                         " 'b': {city:'Venecia',postcode:28756 }," +
                         " 'c': {city:'Barcelona',postcode:28059 }}," +
                         "[" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "], " +
                         "[" +
                         "{{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "], " +
                         "[" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                         "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "]," +
                         "{" +
                         "[{city:'Barcelona',postcode:28059 },{city:'Tarragona',postcode:29506 },{city:'Valencia',postcode: 85964 }]," +
                         "[{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'San Sebastian',postcode: 85964 }]," +
                         "[{city:'Aviles',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Toledo',postcode: 85964 }]" +
                         "}, " +
                         "{" +
                         "{{city:'Valladolid',postcode:28059 },{city:'Salamanca',postcode:29506 },{city:'Valencia',postcode: 85964 }}," +
                         "{{city:'Oviedo',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Jaen',postcode: 85964 }}," +
                         "{{city:'San Sebastian',postcode:28059 },{city:'Madrid',postcode:29506 },{city:'Toledo',postcode: 85964 }}" +
                         "}, " +
                         "{" +
                         "{'a': {city:'Barcelona',postcode:28059 },'b': {city:'Roma',postcode:29506 },'c': {city:'Valencia',postcode: 85964 }}," +
                         "{'d': {city:'Valladolid',postcode:28059 },'e': {city:'Madrid',postcode:29506 },'f': {city:'San Sebastian',postcode: 85964 }}," +
                         "{'g': {city:'Salamanca',postcode:28059 },'h': {city:'Madrid',postcode:29506 },'i': {city:'Toledo',postcode: 85964 }}" +
                         "});";

        cassandraUtils.execute(new SimpleStatement(insert));
        cassandraUtils.execute(new SimpleStatement(insert2));
        cassandraUtils.execute(new SimpleStatement(insert3));
        cassandraUtils.execute(new SimpleStatement(insert4));
        cassandraUtils.execute(new SimpleStatement(insert5));
        cassandraUtils.execute(new SimpleStatement(insert6));
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
        assertTrue("Unexpected results!! Expected: " + Arrays.toString(expected) + ",but got: " + received.toString(),
                   isThisAndOnlyThis(received, expected));

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
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_map$a.city", "Barcelona"));

        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map$b.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_map$c.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_map$a.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$b.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map$c.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$a.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$b.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$c.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_map$a.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER6"});

        select = cassandraUtils.filter(match("address_map$b.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_map$c.city", "Bilbao"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$a.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$b.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER6"});

        select = cassandraUtils.filter(match("address_map$c.city", "Venecia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_map$a.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3",});

        select = cassandraUtils.filter(match("address_map$b.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$c.city", "Lisboa"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_map$a.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$b.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_map$c.city", "Sevilla"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_map$a.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4", "USER5"});

        select = cassandraUtils.filter(match("address_map$b.city", "Granada"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{});

        select = cassandraUtils.filter(match("address_map$c.city", "Granada"));
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
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_list_map$a.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$b.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$f.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_list_map$g.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_map$c.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$d.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_list_map$e.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$h.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$i.city", "Toledo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$c.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_list_map$e.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3"});

        select = cassandraUtils.filter(match("address_list_map$f.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER5"});

        select = cassandraUtils.filter(match("address_list_map$g.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$i.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_list_map$b.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_list_map$c.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_list_map$d.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_list_map$f.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_list_map$h.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_list_map$c.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_list_map$d.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_list_map$i.city", "Valladolid"));
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
        CassandraUtilsSelect select = cassandraUtils.filter(match("address_set_map$a.city", "Barcelona"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$b.city", "Roma"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER4", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$f.city", "Jaen"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4"});

        select = cassandraUtils.filter(match("address_set_map$g.city", "Aviles"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER3", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_map$c.city", "Valencia"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$d.city", "Oviedo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5"});

        select = cassandraUtils.filter(match("address_set_map$e.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2", "USER4", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$h.city", "Madrid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"),
                                      new String[]{"USER1", "USER2", "USER3", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$i.city", "Toledo"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER2", "USER5", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$c.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1"});

        select = cassandraUtils.filter(match("address_set_map$e.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER3"});

        select = cassandraUtils.filter(match("address_set_map$f.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER5"});

        select = cassandraUtils.filter(match("address_set_map$g.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$i.city", "Salamanca"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_set_map$b.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER5"});

        select = cassandraUtils.filter(match("address_set_map$c.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER2"});

        select = cassandraUtils.filter(match("address_set_map$d.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER3"});

        select = cassandraUtils.filter(match("address_set_map$f.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER6"});

        select = cassandraUtils.filter(match("address_set_map$h.city", "San Sebastian"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_set_map$c.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});

        select = cassandraUtils.filter(match("address_set_map$d.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER1", "USER6"});

        select = cassandraUtils.filter(match("address_set_map$i.city", "Valladolid"));
        assertEqualsAndOnlyThisString(select.stringColumn("login"), new String[]{"USER4"});
    }
}
