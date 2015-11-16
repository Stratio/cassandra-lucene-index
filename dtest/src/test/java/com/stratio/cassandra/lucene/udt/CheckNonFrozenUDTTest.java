package com.stratio.cassandra.lucene.udt;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.UDT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CheckNonFrozenUDTTest {
    /**
     * Now, cassandra (2.1.11, 2.2.3 ) demand that User Defined Types and collections Of
     * UDF must be frozen but this may change in next future so i include these tests to
     * be able in next versions to detect this change because it is problematic
     */
    private static CassandraUtils cassandraUtils;
    static final String KEYSPACE_NAME="non_frozen_udt";

    @Test
    public void testNotFrozenUDT() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).withTable(TestingConstants.TABLE_NAME_CONSTANT).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city","text");
        addressUDT.add("postcode","int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address address_udt);";

        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(addressUDT.build());
        try {
            cassandraUtils.execute(tableCreationQuery);
            assertTrue("This must return InvalidQueryException but does not",false);
        } catch (InvalidQueryException e) {
            String expectedMesssage="Non-frozen User-Defined types are not supported, please use frozen<>";
            assertEquals("Getted exception with non expected message",expectedMesssage,e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTList() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).withTable(TestingConstants.TABLE_NAME_CONSTANT).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city","text");
        addressUDT.add("postcode","int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address list<address_udt>);";

        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(addressUDT.build());
        try {
            cassandraUtils.execute(tableCreationQuery);
            assertTrue("This must return InvalidQueryException but does not",false);
        } catch (InvalidQueryException e) {
            String expectedMesssage="Non-frozen collections are not allowed inside collections: list<address_udt>";
            assertEquals("Getted exception with non expected message",expectedMesssage,e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTSet() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).withTable(TestingConstants.TABLE_NAME_CONSTANT).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city","text");
        addressUDT.add("postcode","int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address set<address_udt>);";

        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(addressUDT.build());
        try {
            cassandraUtils.execute(tableCreationQuery);
            assertTrue("This must return InvalidQueryException but does not",false);
        } catch (InvalidQueryException e) {
            String expectedMesssage="Non-frozen collections are not allowed inside collections: set<address_udt>";
            assertEquals("Getted exception with non expected message",expectedMesssage,e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsKey() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).withTable(TestingConstants.TABLE_NAME_CONSTANT).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city","text");
        addressUDT.add("postcode","int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address map<address_udt,int>);";

        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(addressUDT.build());
        try {
            cassandraUtils.execute(tableCreationQuery);
            assertTrue("This must return InvalidQueryException but does not",false);
        } catch (InvalidQueryException e) {
            String expectedMesssage="Non-frozen collections are not allowed inside collections: map<address_udt, int>";
            assertEquals("Getted exception with non expected message",expectedMesssage,e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testNotFrozenUDTMapAsValue() {
        cassandraUtils = CassandraUtils.builder(KEYSPACE_NAME).withTable(TestingConstants.TABLE_NAME_CONSTANT).build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery = " USE " + cassandraUtils.getKeyspace() + " ;";

        UDT addressUDT = new UDT("address_udt");
        addressUDT.add("city","text");
        addressUDT.add("postcode","int");

        String tableCreationQuery = "CREATE TABLE " +
                                    cassandraUtils.getTable() +
                                    " ( login text PRIMARY KEY," +
                                    " address map<int,address_udt>);";

        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(addressUDT.build());
        try {
            cassandraUtils.execute(tableCreationQuery);
            assertTrue("This must return InvalidQueryException but does not",false);
        } catch (InvalidQueryException e) {
            String expectedMesssage="Non-frozen collections are not allowed inside collections: map<int, address_udt>";
            assertEquals("Getted exception with non expected message",expectedMesssage,e.getMessage());

        }
        cassandraUtils.dropKeyspace();
    }
}
