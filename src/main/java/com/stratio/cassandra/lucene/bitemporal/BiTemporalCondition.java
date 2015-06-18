package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalCondition extends Condition {
    /**
     * Abstract {@link Condition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}.
     */
    public BiTemporalCondition(@JsonProperty("boost") Float boost) {
        super(boost);
    }

    @Override
    public Query query(Schema schema) {
        return null;
    }
}
