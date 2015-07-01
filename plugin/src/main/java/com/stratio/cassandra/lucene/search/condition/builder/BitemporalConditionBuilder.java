package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.BitemporalCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link BitemporalCondition}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BitemporalConditionBuilder extends ConditionBuilder<BitemporalCondition, BitemporalConditionBuilder> {

    /** The name of the filed to be matched. */
    @JsonProperty("field")
    String field;

    /** The valid time start. */
    @JsonProperty("vt_from")
    Object vt_from;

    /** The valid time end. */
    @JsonProperty("vt_to")
    Object vt_to;

    /** The transaction time start. */
    @JsonProperty("tt_from")
    Object tt_from;

    /** The transaction time end. */
    @JsonProperty("tt_to")
    Object tt_to;

    /** The spatial operation to be performed. */
    @JsonProperty("operation")
    String operation;

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
     * @param vt_from The valid time start to be set.
     * @return This.
     */
    public BitemporalConditionBuilder setVt_from(Object vt_from) {
        this.vt_from = vt_from;
        return this;
    }

    /**
     * Sets the valid time end.
     *
     * @param vt_to The valid time end to be set.
     * @return This.
     */
    public BitemporalConditionBuilder setVt_to(Object vt_to) {
        this.vt_to = vt_to;
        return this;
    }

    /**
     * Sets the transaction time start.
     *
     * @param tt_from The transaction time start to be set.
     * @return This.
     */
    public BitemporalConditionBuilder setTt_from(Object tt_from) {
        this.tt_from = tt_from;
        return this;
    }

    /**
     * Sets the transaction time end.
     *
     * @param tt_to The transaction time end to be set.
     * @return This.
     */
    public BitemporalConditionBuilder setTt_to(Object tt_to) {
        this.tt_to = tt_to;
        return this;
    }

    /**
     * Sets the spatial operation to be performed.
     *
     * @param operation The spatial operation to be performed.
     * @return This.
     */
    public BitemporalConditionBuilder setOperation(String operation) {
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
        return new BitemporalCondition(boost, field, vt_from, vt_to, tt_from, tt_to, operation);
    }
}
