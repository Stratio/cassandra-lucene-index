package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Date;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalCondition extends Condition {




    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The Valid Time Start. */
    @JsonProperty("vtStart")
    private final Object vtStart;

    /** The Valid Time End. */
    @JsonProperty("vtEnd")
    private final Object vtEnd;

    /** The Transaction Time Start. */
    @JsonProperty("ttStart")
    private final Object ttStart;

    /** The Transaction Time End. */
    @JsonProperty("ttEnd")
    private final Object ttEnd;




    /**
     * Constructs a query selecting all fields that intesects with valid Time and transaction time including limits.
     * <p/>
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}.
     * @param field The name of the field to be matched.
     * @param vtStart The Valid Time Start.
     * @param vtEnd The Valid Time End.
     * @param ttStart The Transaction Time Start.
     * @param ttEnd The Transaction Time End.
     */
    @JsonCreator
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

    /**
     * Returns the Valid Time Start.
     *
     * @return The Valid Time Start.
     */
    public Object getVtStart() { return vtStart; }

    /**
     * Returns the Valid Time End.
     *
     * @return The Valid Time End.
     */
    public Object getVtEnd() { return vtEnd; }

    /**
     * Returns the Transaction Time Start.
     *
     * @return The Transaction Time Start.
     */
    public Object getTtStart() { return ttStart; }

    /**
     * Returns the Transaction Time End.
     *
     * @return The Transaction Time End.
     */
    public Object getTtEnd() { return ttEnd; }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema) {
        //FilteredQuery filterQuery= new FilteredQuery()
        Mapper mapper = schema.getMapper(field);
        if (!(mapper instanceof BiTemporalMapper)) {
            throw new IllegalArgumentException("BiTemporal mapper required");
        }
        BiTemporalMapper biTemporalMapper=(BiTemporalMapper)mapper;
        BiTemporalDateTime vtStart=biTemporalMapper.parseBiTemporalDate(this.vtStart);
        BiTemporalDateTime vtEnd=biTemporalMapper.parseBiTemporalDate(this.vtEnd);
        BiTemporalDateTime ttStart=biTemporalMapper.parseBiTemporalDate(this.ttStart);
        BiTemporalDateTime ttEnd=biTemporalMapper.parseBiTemporalDate(this.ttEnd);
        Sort idSort = new Sort(new SortField("id", SortField.Type.STRING));
        BooleanQuery totalQuery = new BooleanQuery();
        NumberRangePrefixTreeStrategy[] strategies = new NumberRangePrefixTreeStrategy[4];
        for (int i=0;i<4;i++) {
            strategies[i]=biTemporalMapper.getStrategy(i);
        }
        SpatialArgs args;

        if ((!ttStart.isNow()) && (ttEnd.compareTo(vtStart)>=0)) {
            //R1,R2,R3,R4
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[0].getSpatialContext().makeRectangle(0, ttEnd.getTime(), 0, vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[0].makeFilter(args)),BooleanClause.Occur.MUST);
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[1].getSpatialContext().makeRectangle(0, ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[1].makeFilter(args)),BooleanClause.Occur.MUST);
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[2].getSpatialContext().makeRectangle(BiTemporalDateTime.max(ttStart, vtStart).getTime(), ttEnd.getTime(), 0, vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[2].makeFilter(args)),BooleanClause.Occur.MUST);
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[3].getSpatialContext().makeRectangle(ttStart.getTime(), ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[3].makeFilter(args)),BooleanClause.Occur.MUST);

        } else if ((!ttStart.isNow()) && (ttEnd.compareTo(vtStart)<0)) {
            //R2,R4
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[1].getSpatialContext().makeRectangle(0, ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[1].makeFilter(args)),BooleanClause.Occur.MUST);
            args = new SpatialArgs(SpatialOperation.Intersects, strategies[3].getSpatialContext().makeRectangle(ttStart.getTime(), ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
            totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[3].makeFilter(args)),BooleanClause.Occur.MUST);

        } else if (ttStart.isNow()) {
            if (((!vtStart.isMin()) || (!vtEnd.isMax())) && (ttEnd.compareTo(vtStart) >=0)) {
                //R1,R2
                args = new SpatialArgs(SpatialOperation.Intersects, strategies[0].getSpatialContext().makeRectangle(0, ttEnd.getTime(), 0, vtEnd.getTime()));
                totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[0].makeFilter(args)),BooleanClause.Occur.MUST);
                args  = new SpatialArgs(SpatialOperation.Intersects, strategies[1].getSpatialContext().makeRectangle(0, ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
                totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(),strategies[1].makeFilter(args)),BooleanClause.Occur.MUST);
            } else if (((!vtStart.isMin()) || (!vtEnd.isMax())) && (ttEnd.compareTo(vtStart)<0)) {
                //R2
                args  = new SpatialArgs(SpatialOperation.Intersects, strategies[1].getSpatialContext().makeRectangle(0, ttEnd.getTime(), vtStart.getTime(), vtEnd.getTime()));
                totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(), strategies[1].makeFilter(args)), BooleanClause.Occur.MUST);
            } else if ((vtStart.isMin()) &&  (vtEnd.isMax())){ // [vtStart, vtEnd]==[tmin,tmax]])
                //R1,R2
                args  = new SpatialArgs(SpatialOperation.Intersects, strategies[0].getSpatialContext().makeRectangle(BiTemporalDateTime.MIN.getTime(), BiTemporalDateTime.MAX.getTime(), BiTemporalDateTime.MIN.getTime(), BiTemporalDateTime.MAX.getTime()));
                totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(), strategies[0].makeFilter(args)), BooleanClause.Occur.MUST);
                args  = new SpatialArgs(SpatialOperation.Intersects, strategies[1].getSpatialContext().makeRectangle(BiTemporalDateTime.MIN.getTime(), BiTemporalDateTime.MAX.getTime(), BiTemporalDateTime.MIN.getTime(), BiTemporalDateTime.MAX.getTime()));
                totalQuery.add(new FilteredQuery(new MatchAllDocsQuery(), strategies[1].makeFilter(args)), BooleanClause.Occur.MUST);
            }
        }
        return totalQuery;
    }
}
