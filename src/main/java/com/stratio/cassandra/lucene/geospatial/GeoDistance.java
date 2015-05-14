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
package com.stratio.cassandra.lucene.geospatial;

import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Class representing a geographical distance.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistance {

    private final double value; // The quantitative distance value
    private final GeoDistanceUnit unit; // The distance unit

    /**
     * Builds a new {@link GeoDistance} defined by the specified quantitative value and distance unit.
     *
     * @param value The quantitative distance value.
     * @param unit  The distance unit.
     */
    private GeoDistance(double value, GeoDistanceUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    /**
     * Returns the numeric distance value in the specified unit.
     *
     * @param unit The distance unit to be used.
     * @return The numeric distance value in the specified unit.
     */
    public double getValue(GeoDistanceUnit unit) {
        return this.unit.getMetres() * value / unit.getMetres();
    }

    /**
     * Returns the {@link GeoDistance} represented by the specified JSON {@code String}.
     *
     * @param json A {@code String} containing a JSON encoded {@link GeoDistance}.
     * @return The {@link GeoDistance} represented by the specified JSON {@code String}.
     */
    @JsonCreator
    public static GeoDistance create(String json) {
        try {
            for (GeoDistanceUnit geoDistanceUnit : GeoDistanceUnit.values()) {
                for (String name : geoDistanceUnit.getNames()) {
                    if (json.endsWith(name)) {
                        double value = Double.parseDouble(json.substring(0, json.indexOf(name)));
                        return new GeoDistance(value, geoDistanceUnit);
                    }
                }
            }
            double value = Double.parseDouble(json);
            return new GeoDistance(value, GeoDistanceUnit.METRES);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Unparseable distance: " + json);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("value", value).add("unit", unit).toString();
    }
}
