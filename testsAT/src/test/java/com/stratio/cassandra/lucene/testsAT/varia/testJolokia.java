package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.testsAT.util.monitoring.CassandraJolokiaClient;
import org.junit.Test;

import java.util.List;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class testJolokia {

    //TODO remove this class
    private String service="10.200.0.155:8000";

    @Test
    public void test() {
        CassandraJolokiaClient client = new CassandraJolokiaClient(service).connect();
        String s_name= "org.apache.cassandra.db.Tables.system.local";
        client.getAttribute(s_name, "BuiltIndexes");


    }


    @Test
    public void test2() {
        CassandraJolokiaClient client = new CassandraJolokiaClient(service).connect();
        String s_name="org.apache.cassandra.db:type=Tables,keyspace=system,table=local";
        List<String> indexes =(List<String>)client.getAttribute(s_name, "BuiltIndexes");
        System.out.println("finished indexes: "+indexes.toString());
    }



    @Test
    public void test3() {
        CassandraJolokiaClient client = new CassandraJolokiaClient(service).connect();
        String s_name="org.apache.cassandra.db:type=Tables,keyspace=system_auth,table=roles";
        client.getAttribute(s_name, "BuiltIndexes");

    }
}
