package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.BitemporalCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link BitemporalCondition}.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalConditionBuilder extends ConditionBuilder<BitemporalCondition, BitemporalConditionBuilder> {

    /** The name of the filed to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The valid time start. */
    @JsonProperty("vt_from")
    private Object vtFrom;

    /** The valid time end. */
    @JsonProperty("vt_to")
    private Object vtTo;

    /** The transaction time start. */
    @JsonProperty("tt_from")
    private Object ttFrom;

    /** The transaction time end. */
    @JsonProperty("tt_to")
    private Object ttTo;

    /** The spatial operation to be performed. */
    @JsonProperty("operation")
    private String operation;

    /**
     * Returns a new {@link BitemporalConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     */
    @JsonCreator
    public BitemporalConditionBuilder(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the valid time start.
     *
     * @param vtFrom The valid time start to be set.
     * @return This.
     */
    public BitemporalConditionBuilder vtFrom(Object vtFrom) {
        this.vtFrom = vtFrom;
        return this;
    }

    /**
     * Sets the valid time end.
     *
     * @param vtTo The valid time end to be set.
     * @return This.
     */
    public BitemporalConditionBuilder vtTo(Object vtTo) {
        this.vtTo = vtTo;
        return this;
    }

    /**
     * Sets the transaction time start.
     *
     * @param ttFrom The transaction time start to be set.
     * @return This.
     */
    public BitemporalConditionBuilder ttFrom(Object ttFrom) {
        this.ttFrom = ttFrom;
        return this;
    }

    /**
     * Sets the transaction time end.
     *
     * @param ttTo The transaction time end to be set.
     * @return This.
     */
    public BitemporalConditionBuilder ttTo(Object ttTo) {
        this.ttTo = ttTo;
        return this;
    }

    /**
     * Sets the spatial operation to be performed.
     *
     * @param operation The spatial operation to be performed.
     * @return This.
     */
    public BitemporalConditionBuilder operation(String operation) {
        this.operation = operation;
        return this;
    }

    /**
     * Returns the {@link BitemporalCondition} represented by this builder.
     *
     * @return The {@link BitemporalCondition} represented by this builder.
     */
    @Override
    public BitemporalCondition build() {
        return new BitemporalCondition(boost, field, vtFrom, vtTo, ttFrom, ttTo, operation);
    }
}
