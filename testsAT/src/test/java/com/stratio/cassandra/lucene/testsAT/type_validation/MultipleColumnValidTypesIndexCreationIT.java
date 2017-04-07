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
package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsBuilder;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.stratio.cassandra.lucene.testsAT.type_validation.DataHelper.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Parameterized.class)
public class MultipleColumnValidTypesIndexCreationIT extends BaseIT {

    private final String mapperName;
    private final Mapper mapper;
    private final Set<String> requiredColumnNames;
    private final String cqlType;
    private static CassandraUtils utils;
    private CassandraUtilsBuilder builder;

    public MultipleColumnValidTypesIndexCreationIT(String mapperName,
                                                   Mapper mapper,
                                                   Set<String> requiredColumnNames,
                                                   String cqlType) {
        this.mapperName = mapperName;
        this.mapper = mapper;
        this.requiredColumnNames = requiredColumnNames;
        this.cqlType = cqlType;
    }

    @Before
    public void before() {
        builder = CassandraUtils.builder(buildTableName(mapperName, cqlType))
                                .withIndexColumn(null)
                                .withUseNewQuerySyntax(true)
                                .withPartitionKey("pk")
                                .withColumn("pk", "int", null);
    }

    @Test
    public void test() {
        for (String columnName : requiredColumnNames) {
            builder = builder.withColumn(columnName, cqlType, null);
        }

        utils = builder.withTable(buildTableName(mapperName, cqlType))
                       .withIndexName(buildTableName(mapperName, cqlType))
                       .withMapper(mapperName, mapper)
                       .build()
                       .createKeyspace()
                       .createTable()
                       .createIndex();
    }

    @After
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Parameterized.Parameters(name = "{index}: {0} against type {3}.")
    public static Collection regExValues() {
        List<Object[]> possibleValues = new ArrayList<>();
        for (Mapper mapper : multipleColumnMappersAcceptedTypes.keySet()) {
            for (String acceptedType : multipleColumnMappersAcceptedTypes.get(mapper)) {
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, multipleColumnMapperRequiredColumnNames.get(mapper.toString()), acceptedType});
            }
        }
        return possibleValues;
    }
}
