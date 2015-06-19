package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.bitemporal.BiTemporalCondition;


/**
 * {@link ConditionBuilder} for building a new {@link BiTemporalCondition}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalConditionBuilder extends ConditionBuilder<BiTemporalCondition,BiTemporalConditionBuilder> {

    private String field;//the name of the filed to be matched
    private Object vtStart; //Valid Time Start
    private Object vtEnd;//Valid Time End
    private Object ttStart;//Transaction Time Start
    private Object ttEnd;//Transaction Time Start

    /**
     * Returns a new {@link BiTemporalConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     */
    public BiTemporalConditionBuilder(String field) {this.field=field;}

    /**
     * Sets the Valid Time Start.
     * @param vtStart The Valid Time Start.
     * @return This.
     */
    public BiTemporalConditionBuilder setVtStart(Object vtStart) {
        this.vtStart = vtStart;
        return this;
    }

    /**
     * Sets the Valid Time End.
     * @param vtEnd The Valid Time End.
     * @return This.
     */
    public BiTemporalConditionBuilder setVtEnd(Object vtEnd) {
        this.vtEnd = vtEnd;
        return this;
    }

    /**
     * Sets the Transaction Time Start.
     * @param ttStart The Transaction Time Start.
     * @return This.
     */
    public BiTemporalConditionBuilder setTtStart(Object ttStart) {
        this.ttStart = ttStart;
        return this;
    }

    /**
     * Sets the Transaction Time End.
     * @param ttEnd The Transaction Time End.
     * @return This.
     */
    public BiTemporalConditionBuilder setTtEnd(Object ttEnd) {
        this.ttEnd = ttEnd;
        return this;
    }
    /**
     * Returns the {@link BiTemporalCondition} represented by this builder.
     *
     * @return The {@link BiTemporalCondition} represented by this builder.
     */
    @Override
    public BiTemporalCondition build() {
        return new BiTemporalCondition(boost,field,vtStart,vtEnd,ttStart,ttEnd);
    }
}
