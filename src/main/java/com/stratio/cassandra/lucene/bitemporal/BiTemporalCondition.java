package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalCondition extends Condition {




    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    @JsonProperty("vtStart")
    private final Object vtStart;

    @JsonProperty("vtEnd")
    private final Object vtEnd;

    @JsonProperty("ttStart")
    private final Object ttStart;

    @JsonProperty("ttEnd")
    private final Object ttEnd;




    /**
     * Abstract {@link Condition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}.
     */
    public BiTemporalCondition(@JsonProperty("boost") Float boost,
                               @JsonProperty("field") String field,
                               @JsonProperty("vtStart") Object vtStart,
                               @JsonProperty("vtEnd") Object vtEnd,
                               @JsonProperty("ttStart") Object ttStart,
                               @JsonProperty("ttEnd") Object ttEnd) {
        super(boost);
        this.field = field;
        this.vtStart=vtStart;
        this.vtEnd=vtEnd;
        this.ttStart=ttStart;
        this.ttEnd=ttEnd;
    }


    public Object getVtStart() { return vtStart; }

    public Object getVtEnd() { return vtEnd; }

    public Object getTtStart() { return ttStart; }

    public Object getTtEnd() { return ttEnd; }

    @Override
    public Query query(Schema schema) {
        //FilteredQuery filterQuery= new FilteredQuery()



        return null;
    }
}
