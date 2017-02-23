package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Test;
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
public class SingleColumnRejectTypeIndexCreationIT extends BaseIT {

    private final Mapper mapper;
    private final String type;
    private final String expectedExceptionMessage;
    private CassandraUtils utils;

    public SingleColumnRejectTypeIndexCreationIT(Mapper mapper, String type, String expectedExceptionMessage) {
        this.mapper = mapper;
        this.type = type;
        this.expectedExceptionMessage=expectedExceptionMessage;
    }

    @Test
    public void test() {
        utils= CassandraUtils.builder(KEYSPACE_NAME)
                      .withIndexColumn(null)
                      .withUseNewQuerySyntax(true)
                      .withPartitionKey("pk")
                      .withColumn("pk", "int")
                      .withColumn("column", type, mapper)
                      .build()
                      .createKeyspace()
                      .createTable()
                      .createIndex(InvalidConfigurationInQueryException.class, expectedExceptionMessage);
    }

    @After
    public void afterClass() {
        utils.dropKeyspace();
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}.")
    public static Collection regExValues() {
        List<Object[]> possibleValues = new ArrayList<>();
        for (Mapper mapper : singleColumnMappersAcceptedTypes.keySet()) {

            for (String rejectType : Sets.difference(ALL_CQL_TYPES, singleColumnMappersAcceptedTypes.get(mapper)).immutableCopy()) {
                possibleValues.add(new Object[]{mapper, rejectType, buildIndexMessage(mapper, rejectType)});
            }
        }
        return possibleValues;
    }
}
