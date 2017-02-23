package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
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
public class MultipleColumnRejectTypesIndexCreationIT extends BaseIT {

    private final String mapperName;
    private final Mapper mapper;
    private final Set<String> requiredColumnNames;
    private final String cqlType;
    private final String expectedExceptionMessage;
    private CassandraUtils utils;

    public MultipleColumnRejectTypesIndexCreationIT(String mapperName,
                                                    Mapper mapper,
                                                    Set<String> requiredColumnNames,
                                                    String cqlType,
                                                    String expectedExceptionMessage) {
        this.mapperName = mapperName;
        this.mapper = mapper;
        this.requiredColumnNames = requiredColumnNames;
        this.cqlType = cqlType;
        this.expectedExceptionMessage = expectedExceptionMessage;
    }

    @Test
    public void test() {
        CassandraUtilsBuilder builder = CassandraUtils.builder(KEYSPACE_NAME)
                                                      .withIndexColumn(null)
                                                      .withUseNewQuerySyntax(true)
                                                      .withPartitionKey("pk")
                                                      .withColumn("pk", "int");

        for (String columnName : requiredColumnNames) {
            builder = builder.withColumn(columnName, cqlType);
        }

        utils= builder.withMapper(mapperName, mapper)
               .build()
               .createKeyspace()
               .createTable()
               .createIndex(InvalidConfigurationInQueryException.class, expectedExceptionMessage);
    }

    @After
    public void afterClass() {
        utils.dropKeyspace();
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}.")
    public static Collection regExValues() {
        List<Object[]> possibleValues = new ArrayList<>();
        for (Mapper mapper : multipleColumnMappersAcceptedTypes.keySet()) {
            for (String rejectType : Sets.difference(ALL_CQL_TYPES, multipleColumnMappersAcceptedTypes.get(mapper)).immutableCopy()) {
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(),mapper, multipleColumnMapperRequiredColumnNames.get(mapper.toString()), rejectType, buildIndexMessage(mapper, rejectType)});
            }
        }

        for (Mapper mapper : multipleColumnMappersAcceptedTypes.keySet()) {
            for (String acceptedType : multipleColumnMappersAcceptedTypes.get(mapper)) {
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, multipleColumnMapperRequiredColumnNames.get(mapper.toString()), listComposedType(acceptedType), buildIndexMessage(mapper, listComposedType(acceptedType))});
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, multipleColumnMapperRequiredColumnNames.get(mapper.toString()), setComposedType(acceptedType), buildIndexMessage(mapper, setComposedType(acceptedType))});
                possibleValues.add(new Object[]{mapper.getClass().getSimpleName(), mapper, multipleColumnMapperRequiredColumnNames.get(mapper.toString()), mapComposedType(acceptedType), buildIndexMessage(mapper, mapComposedType(acceptedType))});
            }
        }
        return possibleValues;
    }
}
