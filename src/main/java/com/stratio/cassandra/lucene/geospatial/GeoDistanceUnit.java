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

import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Class representing a spatial distance unit.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
enum GeoDistanceUnit {

    MILLIMETRES(0.001, "mm", "millimetres"),
    CENTIMETRES(0.01, "cm", "centimetres"),
    DECIMETRES(0.1, "dm", "decimetres"),
    DECAMETRES(10, "dam", "decametres"),
    HECTOMETRES(100, "hm", "hectometres"),
    KILOMETRES(1000, "km", "kilometres"),
    FOOTS(0.3048, "ft", "foots"),
    YARDS(0.9144, "yd", "yards"),
    INCHES(0.0254, "in", "inches"),
    MILES(1609.344, "mi", "miles"),
    METRES(1, "m", "metres"),
    NAUTICAL_MILES(1850, "M", "NM", "mil", "nautical_miles");

    private final String[] names;
    private final Double metres;

    /**
     * Builds the {@link GeoDistanceUnit} defined by the specified value in metres and the specified identifying names.
     *
     * @param metres The value in metres.
     * @param names  The identifying names.
     */
    GeoDistanceUnit(double metres, String... names) {
        this.names = names;
        this.metres = metres;
    }

    /**
     * Returns the equivalency in metres.
     *
     * @return The equivalency in metres.
     */
    public Double getMetres() {
        return metres;
    }

    /**
     * Returns the identifying names.
     *
     * @return The identifying names.
     */
    public String[] getNames() {
        return names;
    }

    /**
     * Returns the {@link GeoDistanceUnit} represented by the specified {@code String}.
     *
     * @param value The {@code String} representation of the {@link GeoDistanceUnit} to be created.
     * @return The {@link GeoDistanceUnit} represented by the specified {@code String}.
     */
    @JsonCreator
    public static GeoDistanceUnit create(String value) {
        if (value == null) {
            throw new IllegalArgumentException();
        }
        for (GeoDistanceUnit v : values()) {
            for (String s : v.names) {
                if (s.equals(value)) {
                    return v;
                }
            }
        }
        throw new IllegalArgumentException();
    }

}
