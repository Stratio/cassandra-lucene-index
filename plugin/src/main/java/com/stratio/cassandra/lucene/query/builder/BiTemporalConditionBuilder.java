package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.BiTemporalCondition;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * {@link ConditionBuilder} for building a new {@link BiTemporalCondition}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalConditionBuilder extends ConditionBuilder<BiTemporalCondition,BiTemporalConditionBuilder> {

    @JsonProperty("field")
    String field;//the name of the filed to be matched

    @JsonProperty("vt_from")
    Object vt_from; //Valid Time Start

    @JsonProperty("vt_to")
    Object vt_to;//Valid Time End

    @JsonProperty("tt_from")
    Object tt_from;//Transaction Time Start

    @JsonProperty("tt_to")
    Object tt_to;//Transaction Time Start

    /**
     * Returns a new {@link BiTemporalConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     */
    public BiTemporalConditionBuilder(@JsonProperty("field") String field) {this.field=field;}

    /**
     * Sets the Valid Time Start.
     * @param vt_from The Valid Time Start.
     * @return This.
     */
    public BiTemporalConditionBuilder setVt_from(Object vt_from) {
        this.vt_from = vt_from;
        return this;
    }

    /**
     * Sets the Valid Time End.
     * @param vt_to The Valid Time End.
     * @return This.
     */
    public BiTemporalConditionBuilder setVt_to(Object vt_to) {
        this.vt_to = vt_to;
        return this;
    }

    /**
     * Sets the Transaction Time Start.
     * @param tt_from The Transaction Time Start.
     * @return This.
     */
    public BiTemporalConditionBuilder setTt_from(Object tt_from) {
        this.tt_from = tt_from;
        return this;
    }

    /**
     * Sets the Transaction Time End.
     * @param tt_to The Transaction Time End.
     * @return This.
     */
    public BiTemporalConditionBuilder setTt_to(Object tt_to) {
        this.tt_to = tt_to;
        return this;
    }
    /**
     * Returns the {@link BiTemporalCondition} represented by this builder.
     *
     * @return The {@link BiTemporalCondition} represented by this builder.
     */
    @Override
    public BiTemporalCondition build() {
        return new BiTemporalCondition(boost,field, vt_from, vt_to, tt_from, tt_to);
    }
}
