========================================
Stratio’s Cassandra Lucene Index testsAT
========================================

This test project module contains several functional tests (>800) for testing cassandra-lucenen-index functionality.
Everyone can execute this tests against a cassandra cluster with this mode:

mvn -f testsAT/pom.xml -U verify -Dit.host=10.200.0.155 -Dit.monitor_service=jolokia -Dit.monitor_services_url=10.200.0.155:8000,10.200.0.157:8000
monitor_service could be jmx or jolokia