package com.stratio.cassandra.lucene.builder.search.sort;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A simple relevance sorting for a field of a search.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortField extends SortField {

    /**
     * Creates a new {@link SimpleSortField} for the specified field and reverse option.
     *
     * @param field The name of the field to be used for sort.
     */
    @JsonCreator
    public SimpleSortField(@JsonProperty("field") String field) {
        this.field(field);
    }
}
