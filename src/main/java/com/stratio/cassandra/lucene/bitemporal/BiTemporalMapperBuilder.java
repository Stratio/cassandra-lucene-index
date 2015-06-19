package com.stratio.cassandra.lucene.bitemporal;



import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link BiTemporalMapperBuilder}.
 *
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */
public class BiTemporalMapperBuilder extends MapperBuilder<BiTemporalMapper> {

    /** The name of the column containing the vtStart **/
    private final String vtStart;

    /** The name of the column containing the vtEnd **/
    private final String vtEnd;

    /** The name of the column containing the ttStart **/
    private final String ttStart;

    /** The name of the column containing the ttEnd **/
    private final String ttEnd;

    /** pattern of DateTime **/
    private final String pattern;


    @JsonCreator
    public BiTemporalMapperBuilder(@JsonProperty("vtStart") String vtStart,
                                   @JsonProperty("vtEnd") String vtEnd,
                                   @JsonProperty("ttStart") String ttStart,
                                   @JsonProperty("ttEnd") String ttEnd,
                                   @JsonProperty("pattern") String pattern) {
        this.vtStart=vtStart;
        this.vtEnd=vtEnd;
        this.ttStart=ttStart;
        this.ttEnd=ttEnd;
        this.pattern=pattern;
    }

    /**
     *
     * @param name
     * @return
     */
    @Override
    public BiTemporalMapper build(String name) { return new BiTemporalMapper(name,vtStart,vtEnd,ttStart,ttEnd,pattern); }
}
