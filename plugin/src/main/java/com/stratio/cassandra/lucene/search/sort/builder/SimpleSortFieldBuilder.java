package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.SimpleSortField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortFieldBuilder extends SortFieldBuilder<SimpleSortField,SimpleSortFieldBuilder> {

    @JsonCreator
    public SimpleSortFieldBuilder(@JsonProperty("field") String field) {
        this.field(field);
    }

    @Override
    public SimpleSortField build() {
        return new SimpleSortField(field,reverse);
    }
}
