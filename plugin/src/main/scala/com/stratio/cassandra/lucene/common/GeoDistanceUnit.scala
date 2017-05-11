/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.common

import com.fasterxml.jackson.annotation.JsonCreator

/**
  * Builds the [[GeoDistanceUnit]] defined by the specified value in metres and the specified identifying names.
  *
  * @param metres the value in metres
  * @param names the identifying names
 */
class GeoDistanceUnit(metres: Double , names : List[String]) {

    /**
     * Returns the equivalency in metres.
     *
     * @return the equivalency in metres
     */
    def getMetres() : Double = metres

    /**
     * Returns the identifying names.
     *
     * @return the identifying names
     */
    def getNames() : List[String] = names

    /** @inheritdoc */
    override def toString : String = getNames()(1).toUpperCase
}

object GeoDistanceUnit {
    val MILLIMETRES: GeoDistanceUnit = new GeoDistanceUnit(0.001, List("mm", "millimetres"))
    val CENTIMETRES: GeoDistanceUnit = new GeoDistanceUnit(0.01, List("cm", "centimetres"))
    val DECIMETRES: GeoDistanceUnit = new GeoDistanceUnit(0.1, List("dm", "decimetres"))
    val DECAMETRES: GeoDistanceUnit = new GeoDistanceUnit(10, List("dam", "decametres"))
    val HECTOMETRES: GeoDistanceUnit = new GeoDistanceUnit(100, List("hm", "hectometres"))
    val KILOMETRES: GeoDistanceUnit = new GeoDistanceUnit(1000, List("km", "kilometres"))
    val FOOTS: GeoDistanceUnit = new GeoDistanceUnit(0.3048,List( "ft", "foots"))
    val YARDS: GeoDistanceUnit = new GeoDistanceUnit(0.9144, List("yd", "yards"))
    val INCHES: GeoDistanceUnit = new GeoDistanceUnit(0.0254, List("in", "inches"))
    val MILES: GeoDistanceUnit = new GeoDistanceUnit(1609.344, List("mi", "miles"))
    val METRES: GeoDistanceUnit = new GeoDistanceUnit(1, List("m", "metres"))
    val NAUTICAL_MILES: GeoDistanceUnit = new GeoDistanceUnit(1850, List("M", "NM", "mil", "nautical_miles"))

    def values() : List[GeoDistanceUnit] =
        List( MILLIMETRES, CENTIMETRES, DECIMETRES, DECAMETRES, HECTOMETRES, KILOMETRES, FOOTS, YARDS, INCHES, MILES, METRES, NAUTICAL_MILES)

    /**
      * Returns the [[GeoDistanceUnit]] represented by the specified [[String]].
      *
      * @param value the [[String]] representation of the [[GeoDistanceUnit]] to be created
      * @return the [[GeoDistanceUnit]] represented by the specified [[String]]
      */
    @JsonCreator
    def parse(value: String) : GeoDistanceUnit = {
        var returnObject : Option[GeoDistanceUnit]= None
        if (value == null) {
            throw new IllegalArgumentException()
        }
        for (valueItem <- values ()) {
            for (s <- valueItem.getNames()) {
                if (s.equals(value)) {
                    returnObject= Some(valueItem)
                }
            }
        }
        if (returnObject.isDefined) {
            returnObject.get
        } else {
            throw new IllegalArgumentException()
        }
    }
}
