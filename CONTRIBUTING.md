Contributing to Stratio’s Cassandra Lucene Index
================================================

Issues and pull requests are more than welcome.

Bugs should be reported through GitHub issues. Before reporting a bug, please make sure that:

- You have specified the versions of Apache Cassandra and Stratio’s Cassandra Lucene Index that you 
  are using, if applicable. Please note that these versions are different, and they should be 
  compatible. Specific Cassandra Lucene index versions are targeted to specific Apache Cassandra 
  versions. So, cassandra-lucene-index A.B.C.X is aimed to be used with Apache Cassandra A.B.C, e.g.
  [cassandra-lucene-index:3.0.7.1](http://www.github.com/Stratio/cassandra-lucene-index/tree/3.0.7.1) 
  should be used only with 
  [apache-cassandra:3.0.7](http://www.github.com/apache/cassandra/tree/cassandra-3.0.7).
- You have provided reproduction steps. In most cases this includes the definitions of the index and
  the indexed table, a description of the cluster and maybe some test data.
  
Before you send a pull request, please make sure that:

- You have included some tests for your changes, if applicable.
- The changes are mentioned in the [CHANGELOG.md](CHANGELOG.md) file.
- The project still passes all the unit tests, which you can run with `mvn clean test`
- The project still passes all the acceptance tests. These tests are an independent Maven project in
  the directory [testsAT](testsAT/) that runs acceptance tests against a patched Apache Cassandra 
  cluster.