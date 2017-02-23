package com.stratio.cassandra.lucene.testsAT.type_validation;

import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
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
public class SingleColumnValidTypesIndexCreationIT extends BaseIT {

    private final Mapper mapper;
    private final String type;

    public SingleColumnValidTypesIndexCreationIT(Mapper mapper, String type) {
        this.mapper = mapper;
        this.type = type;
    }



    @Test
    public void test() {
        CassandraUtils.builder(KEYSPACE_NAME)
                      .withIndexColumn(null)
                      .withUseNewQuerySyntax(true)
                      .withPartitionKey("pk")
                      .withColumn("pk", "int")
                      .withColumn("column", type, mapper)
                      .build()
                      .createKeyspace()
                      .createTable()
                      .createIndex()
                      .dropKeyspace();
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}.")
    public static Collection regExValues() {
        List<Object[]> possibleValues = new ArrayList<>();
        for (Mapper mapper : singleColumnMappersAcceptedTypes.keySet()) {
            for (String acceptedType : singleColumnMappersAcceptedTypes.get(mapper)) {
                possibleValues.add(new Object[]{mapper, acceptedType});
                possibleValues.add(new Object[]{mapper, listComposedType(acceptedType)});
                possibleValues.add(new Object[]{mapper, setComposedType(acceptedType)});
                possibleValues.add(new Object[]{mapper, mapComposedType(acceptedType)});
            }
        }
        return possibleValues;
    }
}
