package com.stratio.cassandra.lucene.suite;

import com.stratio.cassandra.lucene.udt.CheckNonFrozenUDTTest;
import com.stratio.cassandra.lucene.udt.UDTCollectionsTest;
import com.stratio.cassandra.lucene.udt.UDTIndexingTest;
import com.stratio.cassandra.lucene.udt.UDTValidationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({UDTValidationTest.class,
                     UDTIndexingTest.class,
                     UDTCollectionsTest.class,
                     CheckNonFrozenUDTTest.class})
public class UDTSuite {

}
