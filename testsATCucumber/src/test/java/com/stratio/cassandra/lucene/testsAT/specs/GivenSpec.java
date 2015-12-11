package com.stratio.cassandra.lucene.testsAT.specs;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.datastax.driver.core.ResultSet;
import com.ning.http.client.Response;

import cucumber.api.DataTable;
import cucumber.api.java.en.Given;
import junit.framework.Assert;

public class GivenSpec extends BaseSpec {
    public ResultSet results;
    
    
    /**
     * Create a Mapping.
     * 
     * @param keyspace
     * @param table
     * @throws Exception 
     * 
     */
    @Given("^I create a mapping with index name '(.+?)' with scheme '(.+?)' of type '(.+?)' in table '(.+?)' using magic_column '(.+?)' using keyspace '(.+?)' and this options:$")
    public void createCustomMapping(String index_name, String scheme, String type, String table, String magic_column, String keyspace, DataTable modifications) throws Exception {
        commonspec.getLogger().info("Creating a custom mapping", "");
        String retrievedData = commonspec.retrieveData(scheme, type);
        String modifiedData = commonspec.modifyData(retrievedData, type, modifications).toString();
        String query="CREATE CUSTOM INDEX "+index_name+" ON "+keyspace+"."+table+"("+magic_column+") USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = "+modifiedData;
        System.out.println("mappin"+query);
        commonspec.getCassandraClient().executeQuery(query);
    }
    
    /**
     * Create a basic Mapping.
     * 
     * @param keyspace
     * @param table
     * @throws Exception 
     * 
     */
    @Given("^I create a mapping with index name '(.+?)' in table '(.+?)' using magic_column '(.+?)' using keyspace '(.+?)'$")
    public void createBasicMapping(String index_name, String table, String column, String keyspace) throws Exception {
        commonspec.getLogger().info("Creating a basic mapping", "");
        String query="CREATE INDEX "+index_name+" ON "+table+" ("+column+");";
        System.out.println("basicmappin: "+query);
        commonspec.getCassandraClient().executeQuery(query);
    }
    
    /**
     * Create a Cassandra Keyspace.
     * 
     * @param keyspace
     */
    @Given("^I create a Cassandra keyspace named '(.+)'$")
    public void createCassandraKeyspace(String keyspace) {
        commonspec.getLogger().info("Creating a C* keyspace", "");
        commonspec.getCassandraClient().createKeyspace(keyspace);
    }
    /**
     * Connect to cluster.
     * 
     * @param node
     * @param url
     */
    @Given("^I connect to Cassandra cluster with '(.+)' nodes and this url '(.+)'$")
    public void connect(String node, String url) {
        commonspec.getLogger().info("Connecting to cluster", "");
        commonspec.getCassandraClient().buildCluster();
        commonspec.getCassandraClient().connect();
    }
    

    /**
     * Do a geoBbox search over a cluster
     * 
     * @param table
     * @param datatable
     * @throws Exception 
     */
    @Given("^I send a query with scheme '(.+?)' of type '(.+?)' with magic_column '(.+?)' from table: '(.+?)' using keyspace: '(.+?)' and this modifications:$")
    public void sendQueryOfType(String scheme, String type, String magic_column, String table, String keyspace, DataTable modifications) throws Exception {
        commonspec.getCassandraClient().useKeyspace(keyspace);  
        commonspec.getLogger().info("Starting a query of type ", "");
        String retrievedData = commonspec.retrieveData(scheme, type);
        String modifiedData = commonspec.modifyData(retrievedData, type, modifications).toString();
        String query="SELECT * FROM "+table+" WHERE "+magic_column+" = '"+modifiedData+"';";
        System.out.println("query:"+query);
             
        results=commonspec.getCassandraClient().executeQuery(query);
        
        
    }
    
   
//    /**
//     * Do a geoBbox search over a cluster
//     * 
//     * @param table
//     * @param datatable
//     */
//    @Given("^I send a query with geoBbox search from table: '(.+?)' with keyspace: '(.+?)' and this datatable:$")
//    public void geoBboxSearh(String table, String keyspace, DataTable datatable) {
//        
//        commonspec.getCassandraClient().useKeyspace(keyspace);        
//        commonspec.getLogger().info("Starting a geoBboxSearch", "");
//        String magic_column=datatable.getGherkinRows().get(1).getCells().get(0);
//        double min_latitude=Double.parseDouble(datatable.getGherkinRows().get(1).getCells().get(1));
//        double min_longitude=Double.parseDouble(datatable.getGherkinRows().get(1).getCells().get(2));
//        double max_latitude=Double.parseDouble(datatable.getGherkinRows().get(1).getCells().get(3));
//        double max_longitude=Double.parseDouble(datatable.getGherkinRows().get(1).getCells().get(4));
//        String filter_query=datatable.getGherkinRows().get(1).getCells().get(5);
//        String field=datatable.getGherkinRows().get(1).getCells().get(6);
//
//        results=commonspec.getCassandraClient().geoBboxSearch(table, magic_column, min_latitude, min_longitude, max_latitude, max_longitude, filter_query, field);
//    }
    
    @Given("^There are '(.+?)' results after execute the last query$")
    public void resultsMustBe(String resultNumber) throws Exception {
        if(results!=null){
        Assert.assertEquals("No se han encontrado "+resultNumber+" resultados"
                + " se han encontrado: "+results.all().size(), Integer.parseInt(resultNumber), results.all().size());
        }else{
            throw new Exception("You must send a query after get results");
        }
        }
    
    
    /**
     * Create table
     * 
     * @param table
     * @param datatable
     * @throws Exception 
     */
    @Given("^I create a table named: '(.+?)' using the keyspace: '(.+?)' and this datatable:$")
    public void createTableWithData(String table, String keyspace, DataTable datatable) throws Exception {
        
        commonspec.getCassandraClient().useKeyspace(keyspace);        
        commonspec.getLogger().info("Starting a table creation", "");
        int attrLength=datatable.getGherkinRows().get(0).getCells().size();
        Map<String,String> columns =  new HashMap<String,String>();
        ArrayList<String> pk=new ArrayList<String>();

        for(int i=0; i<attrLength; i++){
        columns.put(datatable.getGherkinRows().get(0).getCells().get(i), datatable.getGherkinRows().get(1).getCells().get(i));    
        if(datatable.getGherkinRows().get(2).getCells().get(i).equalsIgnoreCase("PK")){
            pk.add(datatable.getGherkinRows().get(0).getCells().get(i));
        }
        } 
        if(pk.isEmpty()){
            throw new Exception("A PK is needed");
        }
        commonspec.getCassandraClient().createTableWithData(table, columns, pk);
    }
    
    /**
     * Insert Data
     * 
     * @param table
     * @param datatable
     * @throws Exception 
     */
    @Given("^I insert in keyspace '(.+?)' and table '(.+?)' this data:$")
    public void insertData(String keyspace, String table, DataTable datatable) throws Exception {
        
        commonspec.getCassandraClient().useKeyspace(keyspace);        
        commonspec.getLogger().info("Starting a table creation", "");
        int attrLength=datatable.getGherkinRows().get(0).getCells().size();
        Map<String, Object> fields =  new HashMap<String,Object>();
        for(int e=1; e<datatable.getGherkinRows().size();e++){
        for(int i=0; i<attrLength; i++){
        fields.put(datatable.getGherkinRows().get(0).getCells().get(i), datatable.getGherkinRows().get(e).getCells().get(i));    

        }
        commonspec.getCassandraClient().insertData(keyspace+"."+table, fields);

        }
    }
    
    public GivenSpec(Common spec) {
        this.commonspec = spec;
    }
    

}
