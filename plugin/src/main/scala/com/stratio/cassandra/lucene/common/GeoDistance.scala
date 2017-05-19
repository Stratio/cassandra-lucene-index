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
import com.google.common.base.MoreObjects
import com.spatial4j.core.distance.DistanceUtils

import com.stratio.cassandra.lucene.IndexException

/**
 * Class representing a geographical distance.
 *
 * @param measurementValue The quantitative distance value.
 * @param measurementUnit The distance unit.
 * @author Andres de la Pena `adelapena@stratio.com`
  **/
class GeoDistance(measurementValue: Double, measurementUnit: GeoDistanceUnit) extends Ordering[GeoDistance] {

    /**
     * Returns the numeric distance value in the specified unit.
     *
     * @param unit The distance unit to be used.
     * @return The numeric distance value in the specified unit.
     */
    def getValue(unit  : GeoDistanceUnit ) : Double =
        measurementUnit.getMetres * measurementValue / unit.getMetres

    /**
     * Return the numeric distance value in degrees.
     *
     * @return the degrees
     */
    def getDegrees : Double =
        DistanceUtils.dist2Degrees(getValue(GeoDistanceUnit.KILOMETRES), DistanceUtils.EARTH_MEAN_RADIUS_KM)

    /** @inheritdoc */
    override def toString : String =
      MoreObjects.toStringHelper(this).add("value", measurementValue).add("unit", measurementUnit).toString

    override def hashCode(): Int = {
        val temp = java.lang.Double.doubleToLongBits(measurementValue)
        var result : Int = Long.box(temp ^ (temp >>> 32)).toInt
        result = 31 * result
        result + (if (measurementUnit != null) measurementUnit.hashCode() else 0)
    }

    override def compare(
        x: GeoDistance,
        y: GeoDistance): Int = x.getValue(GeoDistanceUnit.MILLIMETRES).compareTo(y.getValue(GeoDistanceUnit.MILLIMETRES))

    override def equals(that: Any): Boolean =
        that match {
            case that: GeoDistance => this.hashCode == that.hashCode && this.compare(this, that) == 0
            case _ => false
        }

}

object GeoDistance {
    /**
      * Returns the [[GeoDistance]] represented by the specified JSON [[String]].
      *
      * @param json A [[String]] containing a JSON encoded [[GeoDistance]].
      * @return The [[GeoDistance]] represented by the specified JSON [[String]].
      */
    @JsonCreator
    def parse(json: String) : GeoDistance = {
        try {
            var unit: Option[String] = Option(null)
            for (geoDistanceUnit <- GeoDistanceUnit.values()) {
                for (name <- geoDistanceUnit.getNames()) {
                    if (json.endsWith(name) && (unit.isEmpty || unit.get.length() < name.length())) {
                        unit = Option(name)
                    }
                }
            }
            val json_substring = if (unit.isDefined) json.substring(0, json.indexOf(unit.get)) else json
            def value = java.lang.Double.parseDouble(json_substring)
            val unit_ = if (unit.isDefined) GeoDistanceUnit.parse(unit.get) else GeoDistanceUnit.METRES
            new GeoDistance(value, unit_);
        } catch {
            case (e: Exception) => throw new IndexException(e, s"Unparseable distance: $json")
        }
    }
}
