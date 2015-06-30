/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.BiTemporalMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * A {@link Condition} implementation that matches bi-temporal (four) fields within two range of values.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalCondition extends Condition {

    /** The name of the field to be matched. */
    public final String field;

    /** The Valid Time Start. */
    public final Object vt_from;

    /** The Valid Time End. */
    public final Object vt_to;

    /** The Transaction Time Start. */
    public final Object tt_from;

    /** The Transaction Time End. */
    public final Object tt_to;

    /** The spatial operation to be performed. */
    public final String operation;

    public final SpatialOperation spatialOperation;
    public static final String DEFAULT_OPERATION = "contains";


    /**
     * Constructs a query selecting all fields that intersects with valid time and transaction time ranges including limits.
     * <p/>
     *
     * @param boost   The boost for this query clause. Documents matching this clause will (in addition to the normal
     *                weightings) have their score multiplied by {@code boost}.
     * @param field   The name of the field to be matched.
     * @param vt_from The Valid Time Start.
     * @param vt_to   The Valid Time End.
     * @param tt_from The Transaction Time Start.
     * @param tt_to   The Transaction Time End.
     * @param operation The spatial operation to be performed.
     */
    @JsonCreator
    public BiTemporalCondition(Float boost, String field, Object vt_from, Object vt_to, Object tt_from, Object tt_to, String operation) {
        super(boost);
        this.field = field;
        this.vt_from = vt_from;
        this.vt_to = vt_to;
        this.tt_from = tt_from;
        this.tt_to = tt_to;
        this.operation = operation == null ? DEFAULT_OPERATION : operation;
        this.spatialOperation= parseSpatialOperation(this.operation);
    }

    private Query makeNormalQuery(BiTemporalMapper mapper,
                                  NumberRangePrefixTreeStrategy strategy,
                                  DateRangePrefixTree tree,
                                  BiTemporalMapper.BiTemporalDateTime x_from,
                                  BiTemporalMapper.BiTemporalDateTime x_to) {
        SpatialArgs args = new SpatialArgs(this.spatialOperation, mapper.makeShape(tree, x_from, x_to));
        return strategy.makeQuery(args);
    }
    static SpatialOperation parseSpatialOperation(String operation) {
        if (operation == null) {
            throw new IllegalArgumentException("Operation is required");
        } else if (operation.equalsIgnoreCase("contains")) {
            return SpatialOperation.Contains;
        } else if (operation.equalsIgnoreCase("intersects")) {
            return SpatialOperation.Intersects;
        } else if (operation.equalsIgnoreCase("iswithin")) {
            return SpatialOperation.IsWithin;
        } else {
            throw new IllegalArgumentException("Operation is invalid: " + operation);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema) {

        Mapper mapper = schema.getMapper(field);
        if (!(mapper instanceof BiTemporalMapper)) {
            throw new IllegalArgumentException("BiTemporal2 mapper required");
        }
        BiTemporalMapper biTemporalMapper = (BiTemporalMapper) mapper;

        BiTemporalMapper.BiTemporalDateTime vt_from = biTemporalMapper.parseBiTemporalDate(this.vt_from);
        BiTemporalMapper.BiTemporalDateTime vt_to = biTemporalMapper.parseBiTemporalDate(this.vt_to);
        BiTemporalMapper.BiTemporalDateTime tt_from = biTemporalMapper.parseBiTemporalDate(this.tt_from);
        BiTemporalMapper.BiTemporalDateTime tt_to = biTemporalMapper.parseBiTemporalDate(this.tt_to);
        
        BooleanQuery query = new BooleanQuery();
        NumberRangePrefixTreeStrategy[] validTimeStrategies = new NumberRangePrefixTreeStrategy[4];
        DateRangePrefixTree[] validTimeTrees = new DateRangePrefixTree[4];
        NumberRangePrefixTreeStrategy[] transactionTimeStrategies = new NumberRangePrefixTreeStrategy[4];
        DateRangePrefixTree[] transactionTimeTrees = new DateRangePrefixTree[4];
        for (int i = 0; i < 4; i++) {
            validTimeStrategies[i] = biTemporalMapper.getStrategy(i, true);
            transactionTimeStrategies[i] = biTemporalMapper.getStrategy(i, false);
            validTimeTrees[i] = biTemporalMapper.getTree(i, true);
            transactionTimeTrees[i] = biTemporalMapper.getTree(i, false);
        }

        SpatialArgs args;

        if (!tt_from.isNow() && (tt_to.compareTo(vt_from) >= 0)) {
            //R1,R2,R3,R4
         
            Query vQueryT1 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[0],
                                             validTimeTrees[0],
                                             BiTemporalMapper.BiTemporalDateTime.MIN,
                                             vt_to);
            Query tQueryT1 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[0],
                                             transactionTimeTrees[0],
                                             BiTemporalMapper.BiTemporalDateTime.MIN,
                                             tt_to);

            BooleanQuery t1Query = new BooleanQuery();
            t1Query.add(vQueryT1, MUST);
            t1Query.add(tQueryT1, MUST);
            query.add(t1Query,SHOULD);

            Query vQueryT2 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[1],
                                             validTimeTrees[1],
                                             vt_from,
                                             vt_to);
            Query tQueryT2 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[1],
                                             transactionTimeTrees[1],
                                             BiTemporalMapper.BiTemporalDateTime.MIN,
                                             tt_to);
            BooleanQuery t2Query = new BooleanQuery();
            t2Query.add(vQueryT2, MUST);
            t2Query.add(tQueryT2, MUST);
            query.add(t2Query,SHOULD);


            Query vQueryT3 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[2],
                                             validTimeTrees[2],
                                             BiTemporalMapper.BiTemporalDateTime.MIN,
                                             vt_to);
            Query tQueryT3 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[2],
                                             transactionTimeTrees[2],
                                             BiTemporalMapper.BiTemporalDateTime.max(tt_from, vt_from),
                                             tt_to);
            BooleanQuery t3Query= new BooleanQuery();
            t3Query.add(vQueryT3, MUST);
            t3Query.add(tQueryT3, MUST);
            query.add(t3Query,SHOULD);

            Query vQueryT4 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[3],
                                             validTimeTrees[3],
                                             vt_from,
                                             vt_to);
            Query tQueryT4 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[3],
                                             transactionTimeTrees[3],
                                             tt_from,
                                             tt_to);
            BooleanQuery t4Query= new BooleanQuery();
            t4Query.add(vQueryT4, MUST);
            t4Query.add(tQueryT4, MUST);
            query.add(t4Query,SHOULD);

        } else if ((!tt_from.isNow()) && (tt_to.compareTo(vt_from) < 0)) {
            //R2,R4
            Query vQueryT2 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[1],
                                             validTimeTrees[1],
                                             vt_from,
                                             vt_to);
            Query tQueryT2 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[1],
                                             transactionTimeTrees[1],
                                             BiTemporalMapper.BiTemporalDateTime.MIN,
                                             tt_to);
            BooleanQuery t2Query= new BooleanQuery();
            t2Query.add(vQueryT2, MUST);
            t2Query.add(tQueryT2, MUST);
            query.add(t2Query,SHOULD);

            Query vQueryT4 = makeNormalQuery(biTemporalMapper,
                                             validTimeStrategies[3],
                                             validTimeTrees[3],
                                             vt_from,
                                             vt_to);
            Query tQueryT4 = makeNormalQuery(biTemporalMapper,
                                             transactionTimeStrategies[3],
                                             transactionTimeTrees[3],
                                             tt_from,
                                             tt_to);
            BooleanQuery t4Query= new BooleanQuery();
            t4Query.add(vQueryT4, MUST);
            t4Query.add(tQueryT4, MUST);
            query.add(t4Query,SHOULD);

        } else if (tt_from.isNow()) {
            if (((!vt_from.isMin()) || (!vt_to.isMax())) && (tt_to.compareTo(vt_from) >= 0)) {
                //R1,R2
                Query vQueryT1 = makeNormalQuery(biTemporalMapper,
                                                 validTimeStrategies[0],
                                                 validTimeTrees[0],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 vt_to);
                Query tQueryT1 = makeNormalQuery(biTemporalMapper,
                                                 transactionTimeStrategies[0],
                                                 transactionTimeTrees[0],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 tt_to);

                BooleanQuery t1Query= new BooleanQuery();
                t1Query.add(vQueryT1, MUST);
                t1Query.add(tQueryT1, MUST);
                query.add(t1Query,SHOULD);

                Query vQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 validTimeStrategies[1],
                                                 validTimeTrees[1],
                                                 vt_from,
                                                 vt_to);
                Query tQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 transactionTimeStrategies[1],
                                                 transactionTimeTrees[1],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 tt_to);
                BooleanQuery t2Query= new BooleanQuery();
                t2Query.add(vQueryT2, MUST);
                t2Query.add(tQueryT2, MUST);
                query.add(t2Query,SHOULD);

            } else if (((!vt_from.isMin()) || (!vt_to.isMax())) && (tt_to.compareTo(vt_from) < 0)) {
                //R2
                Query vQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 validTimeStrategies[1],
                                                 validTimeTrees[1],
                                                 vt_from,
                                                 vt_to);
                Query tQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 transactionTimeStrategies[1],
                                                 transactionTimeTrees[1],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 tt_to);
                BooleanQuery t2Query= new BooleanQuery();
                t2Query.add(vQueryT2, MUST);
                t2Query.add(tQueryT2, MUST);
                query.add(t2Query,SHOULD);
                
            } else if ((vt_from.isMin()) && (vt_to.isMax())) { // [vt_from, vt_to]==[tmin,tmax]])
                //R1,R2
                Query vQueryT1 = makeNormalQuery(biTemporalMapper,
                                                 validTimeStrategies[0],
                                                 validTimeTrees[0],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 BiTemporalMapper.BiTemporalDateTime.MAX);
                Query tQueryT1 = makeNormalQuery(biTemporalMapper,
                                                 transactionTimeStrategies[0],
                                                 transactionTimeTrees[0],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 BiTemporalMapper.BiTemporalDateTime.MAX);

                BooleanQuery t1Query= new BooleanQuery();
                t1Query.add(vQueryT1, MUST);
                t1Query.add(tQueryT1, MUST);
                query.add(t1Query,SHOULD);

                Query vQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 validTimeStrategies[1],
                                                 validTimeTrees[1],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 BiTemporalMapper.BiTemporalDateTime.MAX);
                Query tQueryT2 = makeNormalQuery(biTemporalMapper,
                                                 transactionTimeStrategies[1],
                                                 transactionTimeTrees[1],
                                                 BiTemporalMapper.BiTemporalDateTime.MIN,
                                                 BiTemporalMapper.BiTemporalDateTime.MAX);

                BooleanQuery t2Query= new BooleanQuery();
                t2Query.add(vQueryT2, MUST);
                t2Query.add(tQueryT2, MUST);
                query.add(t2Query,SHOULD);
            }
        }
        query.setBoost(boost);
        return query;
    }
    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("boost", boost)
                .add("field", field)
                .add("vt_from", vt_from)
                .add("vt_to",vt_to)
                .add("tt_from",tt_from)
                .add("tt_to",tt_to)
                .toString();
    }
}
