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

import static com.stratio.cassandra.lucene.testsAT.type_validation.DataHelper.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Parameterized.class)
public class SingleColumnValidTypesIndexCreationIT extends BaseIT {

    private final String mapperName;
    private final Mapper mapper;
    private final String cqlType;
    private static CassandraUtils utils;
    private static CassandraUtilsBuilder builder;

    public SingleColumnValidTypesIndexCreationIT(String mapperName, Mapper mapper, String cqlType) {
        this.mapperName = mapperName;
        this.mapper = mapper;
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
        utils = builder.withTable(buildTableName(mapperName, cqlType))
                       .withIndexName(buildTableName(mapperName, cqlType))
                       .withColumn("column", cqlType, mapper)
                       .build()
                       .createKeyspace()
                       .createTable()
                       .createIndex();
    }

    @After
    public void after() {
        utils.dropKeyspace();
    }

    @Parameterized.Parameters(name = "{index}: {0} against cqlType {2}.")
    public static Collection regExValues() {
        List<Object[]> possibleValues = new ArrayList<>();
        for (Mapper mapper : singleColumnMappersAcceptedTypes.keySet()) {
            for (String acceptedType : singleColumnMappersAcceptedTypes.get(mapper)) {
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, acceptedType});
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, listComposedType(acceptedType)});
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, setComposedType(acceptedType)});
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, mapComposedType(acceptedType)});
            }
        }
        return possibleValues;
    }
}
