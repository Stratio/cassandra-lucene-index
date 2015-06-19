package com.stratio.cassandra.lucene.bitemporal;

import com.spatial4j.core.shape.Shape;
import com.stratio.cassandra.lucene.schema.Column;
import com.stratio.cassandra.lucene.schema.Columns;

import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalMapper extends Mapper {
    /** The default {@link SimpleDateFormat} pattern. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";
    /** The {@link SimpleDateFormat} pattern. */
    private final String pattern;
    private final String vtStartFieldName;
    private final String vtEndFieldName;
    private final String ttStartFieldName;
    private final String ttEndFieldName;

    private NumberRangePrefixTreeStrategy[] strategies= new NumberRangePrefixTreeStrategy[4];
    private DateRangePrefixTree[] trees= new DateRangePrefixTree[4];

    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> concurrentDateFormat;
    //Builds a new {@link Mapper} supporting the specified types for indexing.

    public BiTemporalMapper(String name,String vtStart,String vtEnd,String ttStart,String ttEnd,String pattern) {
        //TODO accepted types????
        super(name, null,false);
        this.pattern = (pattern == null) ? DEFAULT_PATTERN : pattern;
        this.vtStartFieldName=vtStart;
        this.vtEndFieldName=vtEnd;
        this.ttStartFieldName=ttStart;
        this.ttEndFieldName=ttEnd;

        // Validate pattern
        new SimpleDateFormat(this.pattern);

        for(int i=0;i<4;i++) {
            this.trees[i]= DateRangePrefixTree.INSTANCE;
            this.strategies[i]= new NumberRangePrefixTreeStrategy(this.trees[i],name+ getSuffixForTree(i));
        }

        concurrentDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat(BiTemporalMapper.this.pattern);
            }
        };
    }
    private String getSuffixForTree(int i) {
        return "T"+(i+1);
    }
    @Override
    public void addFields(Document document, Columns columns) {
        BiTemporalDateTime vtStart=readBitemporalDate(columns,this.vtStartFieldName);
        BiTemporalDateTime vtEnd=readBitemporalDate(columns,this.vtEndFieldName);
        BiTemporalDateTime ttStart=readBitemporalDate(columns,this.ttStartFieldName);
        BiTemporalDateTime ttEnd=readBitemporalDate(columns,this.ttEndFieldName);
        Shape shape;
        int i=-1;
        if ((ttEnd.isNow()) &&(vtEnd.isNow())) { // T1
            shape= this.strategies[0].getSpatialContext().makePoint(ttStart.getTime(),vtStart.getTime());
            i=0;
        } else if ((!ttEnd.isNow()) &&(vtEnd.isNow())) {// T2
            shape= this.strategies[1].getSpatialContext().makeRectangle(ttStart.getTime(), ttEnd.getTime(), vtStart.getTime(), vtStart.getTime());
            i=1;
        } else if ((ttEnd.isNow()) && (!vtEnd.isNow())) { // T3
            shape= this.strategies[2].getSpatialContext().makeRectangle(ttStart.getTime(), ttStart.getTime(), vtStart.getTime(), vtEnd.getTime());
            i=2;
        } else { // T4
            shape= this.strategies[3].getSpatialContext().makeRectangle(ttStart.getTime(),ttEnd.getTime(),vtStart.getTime(),vtEnd.getTime());
            i=3;
        }
        for (IndexableField field : this.strategies[i].createIndexableFields(shape)) {
            document.add(field);
        }
    }
/*    private Document newSampleDocument(int i, UUID id,String item, Shape shape) {
        Document doc = new Document();
        //doc.add(new StoredField("id", id.toString()));
        doc.add(new StringField("id", id.toString(), Field.Store.YES));
        doc.add(new StringField("item", item, Field.Store.YES));
        //TODO ADD term
        //Potentially more than one shape in this field is supported by some
        // strategies; see the javadocs of the SpatialStrategy impl to see.

        for (Field f : this.strategies[i].createIndexableFields(shape)) {
            doc.add(f);
        }
        //store it too; the format is up to you
        //  (assume point in this example)
        if (shape.hasArea()) {
            Rectangle rectangle=(Rectangle)shape;
            doc.add(new StoredField(strategies[i].getFieldName(),rectangle.getMinX() + ":" + rectangle.getMaxX() + "," + rectangle.getMinY() + ":" + rectangle.getMaxY()))
            ;
        } else {
            Point point=(Point) shape;
            doc.add(new StoredField(strategies[i].getFieldName(),point.getX() + ":NOW," + point.getY() + ":NOW"));
        }
        return doc;
    }*/
    BiTemporalDateTime readBitemporalDate(Columns columns,String fieldName) {
        Column column = columns.getColumnsByName(fieldName).getFirst();
        if (column == null) {
            throw new IllegalArgumentException(fieldName+" column required");
        }
        Object columnValue = column.getComposedValue();
        Long btDateValue = null;

        if (columnValue != null) {
            if (columnValue instanceof Number) {
                btDateValue = ((Number) columnValue).longValue();
            } else if (columnValue instanceof String) {
                try {


                    btDateValue=concurrentDateFormat.get().parse(columnValue.toString()).getTime();
                } catch (ParseException e) {
                    // Ignore to fail below
                }
            }
            //TODO return error
            //return error("Field '%s' requires a date with format '%s', but found '%s'", name, pattern, btDateValue);
        }
        if (btDateValue == null || btDateValue < 0) {
            throw new IllegalArgumentException("Valid DateTime required, but found " + btDateValue);
        }

        return new BiTemporalDateTime(btDateValue);
    }
    @Override
    public SortField sortField(boolean reverse) {
        return new SortField(name, SortField.Type.LONG, reverse);
    }

    @Override
    public void validate(CFMetaData metaData) {
        validate(metaData, vtStartFieldName);
        validate(metaData, vtEndFieldName);
        validate(metaData, ttStartFieldName);
        validate(metaData, ttEndFieldName);
    }
}
