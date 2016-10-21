package com.stratio.cassandra.lucene.util;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;

/**
 * Utils for Lucene's numeric queries.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class NumericQueryUtils {

    public static Query newIntRange(String field,
                                    Integer start,
                                    Integer stop,
                                    boolean includeStart,
                                    boolean includeStop) {
        if (start == null) {
            start = Integer.MIN_VALUE;
        } else if (!includeStart) {
            start = start + 1;
        }
        if (stop == null) {
            stop = Integer.MAX_VALUE;
        } else if (!includeStop) {
            stop = stop - 1;
        }
        return IntPoint.newRangeQuery(field, start, stop);
    }

    public static Query newLongRange(String field,
                                     Long start,
                                     Long stop,
                                     boolean includeStart,
                                     boolean includeStop) {
        if (start == null) {
            start = Long.MIN_VALUE;
        } else if (!includeStart) {
            start = start + 1;
        }
        if (stop == null) {
            stop = Long.MAX_VALUE;
        } else if (!includeStop) {
            stop = stop - 1;
        }
        return LongPoint.newRangeQuery(field, start, stop);
    }

    public static Query newFloatRange(String field,
                                      Float start,
                                      Float stop,
                                      boolean includeStart,
                                      boolean includeStop) {
        if (start == null) {
            start = Float.NEGATIVE_INFINITY;
        } else if (!includeStart) {
            start = Math.nextUp(start);
        }
        if (stop == null) {
            stop = Float.POSITIVE_INFINITY;
        } else if (!includeStop) {
            stop = Math.nextDown(stop);
        }
        return FloatPoint.newRangeQuery(field, start, stop);
    }

    public static Query newDoubleRange(String field,
                                       Double start,
                                       Double stop,
                                       boolean includeStart,
                                       boolean includeStop) {
        if (start == null) {
            start = Double.NEGATIVE_INFINITY;
        } else if (!includeStart) {
            start = Math.nextUp(start);
        }
        if (stop == null) {
            stop = Double.POSITIVE_INFINITY;
        } else if (!includeStop) {
            stop = Math.nextDown(stop);
        }
        return DoublePoint.newRangeQuery(field, start, stop);
    }
}