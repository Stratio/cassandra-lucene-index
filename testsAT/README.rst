=================================================
Stratio’s Cassandra Lucene Index acceptance tests
=================================================

This project contains several functional tests (>800) for testing cassandra-lucene-index functionality.
Tests can be executed against a patched Apache Cassandra cluster this way:

.. code-block:: bash

    mvn -f testsAT/pom.xml -U verify -Dit.host=10.200.0.155 -Dit.monitor_service=jolokia -Dit.monitor_services_url=10.200.0.155:8000,10.200.0.157:8000

where *monitor_service* could be JMX or Jolokia
