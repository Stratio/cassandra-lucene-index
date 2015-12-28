package com.stratio.cassandra.lucene.builder.search.sort;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A geo spatial distance search sort.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    @JsonProperty("longitude")
    private final double longitude;

    @JsonProperty("latitude")
    private final double latitude;

    /**
     * Creates a new {@link GeoDistanceSortField} for the specified field and reverse option.
     *
     * @param field The name of the field to be used for sort.
     * @param longitude The longitude in degrees of the point to min distance sort by.
     * @param latitude The latitude in degrees of the point to min distance sort by.
     */
    @JsonCreator
    public GeoDistanceSortField(@JsonProperty("field") String field,
                                @JsonProperty("longitude") double longitude,
                                @JsonProperty("latitude") double latitude) {
        super(field);
        this.longitude=longitude;
        this.latitude=latitude;
    }
}
