package com.stratio.cassandra.lucene.bitemporal;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalDateTime implements Comparable{

    private Long dateUnix= null;
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";
    public static BiTemporalDateTime MAX = new BiTemporalDateTime(Long.MAX_VALUE);
    public static BiTemporalDateTime MIN = new BiTemporalDateTime(0L);

    public BiTemporalDateTime(Date date) {
        this.dateUnix=date.getTime();
    }
    public BiTemporalDateTime(Long unixTime) {
        this.dateUnix=unixTime;

    }
    public boolean isNow() {
        return this.dateUnix.equals(MAX);
    }
    public boolean isMin() {
        return this.dateUnix.equals(0L);
    }
    public boolean isMax() {
        return this.dateUnix.equals(MAX);
    }
    public long getTime() {
        return this.dateUnix;
    }
    @Override
    public int compareTo(Object o) {
        BiTemporalDateTime other=(BiTemporalDateTime) o;
        return this.dateUnix.compareTo(other.getTime());
    }

    public static BiTemporalDateTime max(BiTemporalDateTime bt1,BiTemporalDateTime bt2) {
        int result=bt1.compareTo(bt2);
        if (result<=0) {
            return bt2;
        } else {
            return bt1;
        }
    }
    public String toString() {
        Date date= new Date(this.dateUnix);
        return new SimpleDateFormat(DEFAULT_PATTERN).format(date);
    }
}
