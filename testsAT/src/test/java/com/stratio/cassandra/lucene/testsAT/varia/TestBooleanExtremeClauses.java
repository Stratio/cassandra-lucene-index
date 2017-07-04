package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.builder.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.bool;
import static com.stratio.cassandra.lucene.builder.Builder.range;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class TestBooleanExtremeClauses extends BaseIT {

    private static final int NUM_PARTITIONS = 100;
    private static final int NUM_MAX_CLAUSES = 1000;
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("stateless_search_skinny")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int")
                              .withColumn("rc", "int")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (Integer i = 0; i < NUM_PARTITIONS; i++) {
            Map<String, String> data = new LinkedHashMap<>();
            data.put("pk", i.toString());
            data.put("rc", i.toString());
            utils.insert(data);
        }
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testQuery() throws Exception {
        BooleanCondition bool= bool().maxClauses(NUM_MAX_CLAUSES + 1);
        for (int i=0;i< NUM_MAX_CLAUSES ; i++ ) {
            bool.must(range("rc").lower(0).upper(i).includeLower(true).includeUpper(true));
        }
        utils.query(bool).check(1);
    }
}
