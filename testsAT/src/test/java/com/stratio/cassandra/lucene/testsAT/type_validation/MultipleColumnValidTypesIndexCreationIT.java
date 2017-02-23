package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsBuilder;
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
public class MultipleColumnValidTypesIndexCreationIT extends BaseIT {

    private final String mapperName;
    private final Mapper mapper;
    private final Set<String> requiredColumnNames;
    private final String cqlType;


    public MultipleColumnValidTypesIndexCreationIT(String mapperName,
                                                   Mapper mapper,
                                                   Set<String> requiredColumnNames,
                                                   String cqlType) {
        this.mapperName=mapperName;
        this.mapper = mapper;
        this.requiredColumnNames = requiredColumnNames;
        this.cqlType = cqlType;
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

        builder.withMapper(mapperName, mapper)
               .build()
               .createKeyspace()
               .createTable()
               .createIndex()
               .dropKeyspace();
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}.")
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
