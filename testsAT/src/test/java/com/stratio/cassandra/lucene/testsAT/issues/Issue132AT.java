package com.stratio.cassandra.lucene.testsAT.issues;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.stratio.cassandra.lucene.testsAT.*;
import com.stratio.cassandra.lucene.testsAT.util.*;

/**
 * @author Eduardo Alonso {@literal }<eduardoalonso@stratio.com>}.
 */
@RunWith(JUnit4.class)
public class Issue132AT extends BaseAT {

    @Test
    public void testUpdateSetRemovingUniqueElemWide() {
        CassandraUtils utils = builder("issue_132_set_wide").withTable("test_wide_set")
                .withIndex("index_set_wide")
                .withColumn("name", "text")
                .withColumn("sec", "text")
                .withColumn("myset", "set<text>", stringMapper())
                .withColumn("lucene", "text")
                .withPartitionKey("name")
                .withClusteringKey("sec")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name,sec,myset) VALUES ('test1',"
                + "'continue', {'home'});");
        utils.filter(wildcard("myset","hom*")).refresh(true).check(1);
        utils.query(wildcard("myset","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET myset = myset - {'home'} WHERE "
                + "name='test1' AND sec = 'continue';");
        utils.filter(wildcard("myset","hom*")).refresh(true).check(0);
        utils.query(wildcard("myset","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();

    }
    @Test
    public void testUpdateListRemovingUniqueElemWide() {
        CassandraUtils utils = builder("issue_132_list_wide").withTable("test_wide_list")
                .withIndex("index_list_wide")
                .withColumn("name", "text")
                .withColumn("sec", "text")
                .withColumn("mylist", "list<text>", stringMapper())
                .withColumn("lucene", "text")
                .withPartitionKey("name")
                .withClusteringKey("sec")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name, sec,mylist) VALUES ('test1',"
                + "'continue', ['home']);");
        utils.filter(wildcard("mylist","hom*")).refresh(true).check(1);
        utils.query(wildcard("mylist","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET mylist = mylist - ['home'] WHERE "
                + "name='test1' AND sec = 'continue';");
        utils.filter(wildcard("mylist","hom*")).refresh(true).check(0);
        utils.query(wildcard("mylist","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();
    }
    @Test
    public void testUpdateMapRemovingUniqueElemWide() {
        CassandraUtils utils = builder("issue_132_map_wide").withTable("test_wide_map")
                .withIndex("index_map_wide")
                .withPartitionKey("name")
                .withClusteringKey("sec")
                .withColumn("name", "text")
                .withColumn("sec", "text")
                .withColumn("mymap", "map<text,text>", stringMapper())
                .withColumn("lucene", "text")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name, sec,mymap) VALUES ('test1',"
                + "'continue', {'home':'home'});");
        utils.filter(wildcard("mymap$home","hom*")).refresh(true).check(1);
        utils.query(wildcard("mymap$home","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET mymap = mymap - {'home','home'} WHERE "
                + "name='test1' AND sec = 'continue';");
        utils.filter(wildcard("mymap$home","hom*")).refresh(true).check(0);
        utils.query(wildcard("mymap$home","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testUpdateSetRemovingUniqueElemSkinny() {
        CassandraUtils utils = builder("issue_132_set_skinny").withTable("test_skinny_set")
                .withIndex("index_set_skinny")
                .withColumn("name", "text")
                .withColumn("myset", "set<text>", stringMapper())
                .withColumn("lucene", "text")
                .withPartitionKey("name")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name, myset) VALUES ('test1',"
                + "{'home'});");

        utils.filter(wildcard("myset","hom*")).refresh(true).check(1);
        utils.query(wildcard("myset","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET myset = myset - {'home'} WHERE "
                + "name='test1';");
        utils.filter(wildcard("myset","hom*")).refresh(true).check(0);
        utils.query(wildcard("myset","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();

    }
    @Test
    public void testUpdateListRemovingUniqueElemSkinny() {
        CassandraUtils utils = builder("issue_132_list_skinny").withTable("test_skinny_list")
                .withIndex("index_list_skinny")
                .withPartitionKey("name")
                .withColumn("name", "text")
                .withColumn("mylist", "list<text>", stringMapper())
                .withColumn("lucene", "text")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name, mylist) VALUES ('test1',"
                + " ['home']);");
        utils.filter(wildcard("mylist","hom*")).refresh(true).check(1);
        utils.query(wildcard("mylist","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET mylist = mylist - ['home'] WHERE "
                + "name='test1';");
        utils.filter(wildcard("mylist","hom*")).refresh(true).check(0);
        utils.query(wildcard("mylist","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();
    }
    @Test
    public void testUpdateMapRemovingUniqueElemSkinny() {
        CassandraUtils utils = builder("issue_132_map_skinny").withTable("test_skinny_map")
                .withIndex("index_map_skinny")
                .withPartitionKey("name")
                .withColumn("name", "text")
                .withColumn("mymap", "map<text,text>", stringMapper())
                .withColumn("lucene", "text")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .refresh();
        utils.execute("INSERT into "+utils.getKeyspace()+"."+utils.getTable()+ " (name, mymap) VALUES ('test1',"
                + " {'home':'home'});");
        utils.filter(wildcard("mymap$home","hom*")).refresh(true).check(1);
        utils.query(wildcard("mymap$home","hom*")).refresh(true).check(1);


        utils.execute("UPDATE "+utils.getKeyspace()+"."+utils.getTable()+ " SET mymap = mymap - {'home','home'} WHERE "
                + "name='test1';");
        utils.filter(wildcard("mymap","hom*")).refresh(true).check(0);
        utils.query(wildcard("mymap","hom*")).refresh(true).check(0);
        utils.dropIndex().dropTable().dropKeyspace();
    }
}
