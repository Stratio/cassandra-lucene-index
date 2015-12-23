package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.GeoDistanceSortField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortFieldBuilder extends SortFieldBuilder<GeoDistanceSortField,GeoDistanceSortFieldBuilder> {

    @JsonProperty("longitude")
    private final double longitude;

    @JsonProperty("latitude")
    private final double latitude;

    @JsonCreator
    public GeoDistanceSortFieldBuilder(@JsonProperty("field") String field,
                                       @JsonProperty("longitude") double longitude,
                                       @JsonProperty("latitude") double latitude) {


        this.field(field);
        this.longitude=longitude;
        this.latitude=latitude;
    }

    @Override
    public GeoDistanceSortField build() {
        return new GeoDistanceSortField(field, reverse,longitude,latitude);
    }
}
