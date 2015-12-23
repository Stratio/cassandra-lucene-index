package com.stratio.cassandra.lucene.search.sort;

import com.google.common.base.Objects;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {
    public static final Logger logger = LoggerFactory.getLogger(GeoDistanceSortField.class);
    private final String field;
    private final double longitude;
    private final double latitude;
    /**
     * Returns a new {@link SortField}.
     *
     * @param reverse {@code true} if natural order should be reversed.
     */
    public GeoDistanceSortField(String field, Boolean reverse,double longitude, double latitude) {
        super(reverse);
        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }
        this.field=field;
        this.longitude=GeoPointMapper.checkLongitude("longitude", longitude);
        this.latitude=GeoPointMapper.checkLatitude("latitude", latitude);
    }

    public String getField() {
        return field;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        final Mapper mapper = schema.getMapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for sortFields field '%s'", field);
        } else if (!mapper.sorted) {
            throw new IndexException("Mapper '%s' is not sorted", mapper.field);
        } else  if (!(mapper instanceof GeoPointMapper)) {
            throw new IndexException("Only Geo Point Mapper is allowed but Mapper '%s' is not", mapper.field);
        }
        GeoPointMapper geoPointMapper=(GeoPointMapper)mapper;

        SpatialStrategy strategy=geoPointMapper.getDistanceStrategy();
        Point pt = GeoPointMapper.SPATIAL_CONTEXT.makePoint(longitude,latitude);

        ValueSource valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM);//the distance (in km)
        return valueSource.getSortField(this.reverse);
    }

    @Override
    public Comparator<Columns> comparator(Schema schema) {
        final Mapper mapper = schema.getMapper(field);
        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {
                return GeoDistanceSortField.this.compare((GeoPointMapper)mapper, o1, o2);
            }
        };
    }
    protected int compare(GeoPointMapper mapper, Columns o1, Columns o2) {

        if (o1 == null) {
            return o2 == null ? 0 : 1;
        } else if (o2 == null) {
            return -1;
        }

        Double longO1=mapper.readLongitude(o1);
        Double latO1=mapper.readLatitude(o1);

        Double longO2=mapper.readLongitude(o2);
        Double latO2=mapper.readLatitude(o2);


        Double base1 =distance(longO1,latO1);
        Double base2 = distance(longO2,latO2);

        logger.debug("GeoDistanceSortField comparing center=["+longitude+", "+latitude+"] distance(["+longO1+" , "+latO1+" ])="+base1+ "distance(["+longO2+" , "+latO2+" ])="+base2);
        int result=compare(base1, base2);
        logger.debug("GeoDistanceSortField result: "+result);
        return result;
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("reverse", reverse).add("longitude", longitude).add("latitude", latitude).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoDistanceSortField other = (GeoDistanceSortField) o;
        return reverse == other.reverse && field.equals(other.field) && longitude== other.longitude && latitude==other.latitude;
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        result = 31 * result + new Double(latitude).hashCode();
        result = 31 * result + new Double(longitude).hashCode();
        return result;
    }

    public Double distance (Double oLon, Double oLat) {
        if ((oLon==null) || (oLat==null)) return null;
        return DistanceUtils.distHaversineRAD(DistanceUtils.toRadians(latitude),
                                              DistanceUtils.toRadians(longitude),
                                              DistanceUtils.toRadians(oLat),
                                              DistanceUtils.toRadians(oLon));
    }
}
