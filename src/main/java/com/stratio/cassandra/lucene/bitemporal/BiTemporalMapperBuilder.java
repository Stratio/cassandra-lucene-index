package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.schema.mapping.builder.ColumnMapperBuilder;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by eduardoalonso on 18/06/15.
 */
public class BiTemporalMapperBuilder extends ColumnMapperBuilder<BiTemporalMapper> {


    @JsonProperty("vtStart")
    private String vtStart;

    @JsonProperty("vtEnd")
    private String vtEnd;

    @JsonProperty("ttStart")
    private String ttStart;

    @JsonProperty("ttEnd")
    private String ttEnd;

    @JsonProperty("pattern")
    private String pattern;

    public BiTemporalMapperBuilder setVtStart(String vtStart) {
        this.vtStart = vtStart;
        return this;
    }

    public BiTemporalMapperBuilder setVtEnd(String vtEnd) {
        this.vtEnd = vtEnd;
        return this;
    }

    public BiTemporalMapperBuilder setTtStart(String ttStart) {
        this.ttStart = ttStart;
        return this;
    }

    public BiTemporalMapperBuilder setTtEnd(String ttEnd) {
        this.ttEnd = ttEnd;
        return this;
    }

    public BiTemporalMapperBuilder setPattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    @Override
    public BiTemporalMapper build(String name) {
        return new BiTemporalMapper(name,vtStart,vtEnd,ttStart,ttEnd,pattern);
    }
}
