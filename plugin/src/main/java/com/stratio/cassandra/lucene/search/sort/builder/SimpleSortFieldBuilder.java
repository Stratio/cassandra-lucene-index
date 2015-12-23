package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.SimpleSortField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortFieldBuilder extends SortFieldBuilder<SimpleSortField,SimpleSortFieldBuilder> {

    /** The name of the field to be used for sort. */
    @JsonProperty("field")
    private final String field;


    @JsonCreator
    public SimpleSortFieldBuilder(@JsonProperty("field") String field) {
        this.field=field;
    }

    @Override
    public SimpleSortField build() {
        return new SimpleSortField(field,reverse);
    }
}
