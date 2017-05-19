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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.codehaus.jackson.annotate.JsonSubTypes.Type
import com.google.common.base.MoreObjects
import com.spatial4j.core.shape.jts.JtsGeometry
import com.stratio.cassandra.lucene.common.GeospatialUtilsJTS.CONTEXT

/**
 * Class representing the transformation of a JTS geographical shape into a new shape.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */

object GeoTransformations {


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes(
                    Array(
                        new JsonSubTypes.Type(value = classOf[GeoTransformations.BBox], name = "bbox"),
                        new JsonSubTypes.Type(value = classOf[GeoTransformations.Buffer], name = "buffer"),
                        new JsonSubTypes.Type(value = classOf[GeoTransformations.Centroid], name = "centroid"),
                        new JsonSubTypes.Type(value = classOf[GeoTransformations.ConvexHull], name = "convex_hull")
                    )
    )
    trait GeoTransformation {
        /**
          * Returns the [[JtsGeometry]] resulting of applying this transformation to the specified [[JtsGeometry}.
          *
          * @param shape the JTS shape to be transformed
          * @return the transformed JTS shape
          */
        def apply (shape : JtsGeometry) : JtsGeometry
    }

    /**
      * [[GeoTransformation} that returns the bounding box of a JTS geographical shape. The bounding box of shape is
      * the minimal rectangle containing the shape.
      */
    class BBox extends GeoTransformation {
        /**
          * Returns the bounding box of the specified [[JtsGeometry}.
          *
          * @param shape the JTS shape to be transformed
          * @return the convex hull
          */
        override def apply(shape : JtsGeometry) : JtsGeometry =
          CONTEXT.makeShape(CONTEXT.getGeometryFrom(shape.getBoundingBox))


        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).toString
    }

    /**
      * [[GeoTransformation]] that returns the bounding shape of a JTS geographical shape.
      */
    class Buffer @JsonCreator() (@JsonProperty("min_distance") val minDistance: GeoDistance,
                                 @JsonProperty("max_distance") val maxDistance: GeoDistance)  extends GeoTransformation {

        /**
          * Returns the buffer of the specified [[JtsGeometry}.
          *
          * @param shape the JTS shape to be transformed
          * @return the buffer
          */
        override def apply(shape : JtsGeometry) : JtsGeometry = {
            val max = if (maxDistance == null) CONTEXT.makeShape(shape.getGeom)
                      else shape.getBuffered (maxDistance.getDegrees, CONTEXT)

            if (minDistance != null) {
                val min = shape.getBuffered(minDistance.getDegrees, CONTEXT)
                val difference = max.getGeom.difference(min.getGeom)
                CONTEXT.makeShape(difference)
            } else {
              max
            }
        }

        override def toString() : String =
          MoreObjects.toStringHelper(this).add("minDistance", minDistance).add("maxDistance", maxDistance).toString

    }

    /**
      * [[GeoTransformation} that returns the center point of a JTS geographical shape.
      */
    class Centroid extends GeoTransformation {

        /**
          * Returns the center of the specified [[JtsGeometry}.
          *
          * @param shape the JTS shape to be transformed
          * @return the center
          */
        override def apply(shape : JtsGeometry) : JtsGeometry =
            CONTEXT.makeShape(shape.getGeom().getCentroid())

        /** {@inheritDoc } */
        override def toString() : String =
          MoreObjects.toStringHelper(this).toString

    }

    /**
      * [[GeoTransformation} that returns the convex hull of a JTS geographical shape.
      */
    class ConvexHull extends GeoTransformation {

        /**
          * Returns the convex hull of the specified [[JtsGeometry}.
          *
          * @param shape the JTS shape to be transformed
          * @return the convex hull
          */
        override def apply(shape : JtsGeometry) : JtsGeometry =
          CONTEXT.makeShape(shape.getGeom().convexHull())



        /** {@inheritDoc } */
        override def toString() : String = MoreObjects.toStringHelper(this).toString
    }





}