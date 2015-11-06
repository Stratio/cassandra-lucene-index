Query Builder
*************

There is a separate module named "builder" that can be included in client applications
to ease the building of the JSON statements used by the index.
If you are using Maven you can use it by adding this dependency to your pom.xml:

.. code-block:: xml

    <dependency>
        <groupId>com.stratio.cassandra</groupId>
        <artifactId>cassandra-lucene-index-builder</artifactId>
        <version>PLUGIN_VERSION</version>
    </dependency>

Then you can build an index creation statement this way:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    session.execute(index("messages", "lucene").name("my_index")
                                               .refreshSeconds(10)
                                               .defaultAnalyzer("english")
                                               .analyzer("danish", snowballAnalyzer("danish"))
                                               .mapper("id", uuidMapper())
                                               .mapper("user", stringMapper().caseSensitive(false))
                                               .mapper("message", textMapper().analyzer("danish"))
                                               .mapper("date", dateMapper().pattern("yyyyMMdd"))
                                               .build());

And you can also build searches in a similar fashion:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    Search search = search().filter(match("user", "adelapena"))
                            .query(phrase("message", "cassandra rules"))
                            .sort(field("date").reverse(true))
                            .refresh(true);
    ResultSet rs = session.execute(select().from("table").where(eq(indexColumn, search.build()));

