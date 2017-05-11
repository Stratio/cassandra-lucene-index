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
import com.google.common.base.MoreObjects
import com.spatial4j.core.shape.jts.JtsGeometry
import com.stratio.cassandra.lucene.common.GeospatialUtilsJTS.CONTEXT
import com.stratio.cassandra.lucene.common.GeospatialUtilsJTS.geometry

object GeoShapes {

    /**
      * Class representing the transformation of a JTS geographical shape into a new shape.
      *
      * @author Andres de la Pena `adelapena@stratio.com`
      */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = classOf[GeoShapes.WKT])
    @JsonSubTypes(Array(
            new JsonSubTypes.Type(value = classOf[GeoShapes.WKT], name = "wkt"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.BBox], name = "bbox"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.Buffer], name = "buffer"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.Centroid], name = "centroid"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.ConvexHull], name = "convex_hull"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.Difference], name = "difference"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.Intersection], name = "intersection"),
            new JsonSubTypes.Type(value = classOf[GeoShapes.Union], name = "union")
        ))
    trait GeoShape {
        /**
          * Returns the [[JtsGeometry]] resulting of applying this transformation to the specified [[JtsGeometry]].
          *
          * @return the transformed JTS shape
          */
        def apply : JtsGeometry
    }

    /**
      * [[GeoShape]] that returns the bounding box of a JTS geographical shape. The bounding box of shape is
      * the minimal rectangle containing the shape.
      */
    class WKT @JsonCreator() (@JsonProperty("value") val value: String) extends GeoShapes.GeoShape {

        /**
          * Returns the [[JtsGeometry]] represented by the WKT string.
          *
          * @return the shape represented by the WKT string
          */
        override def apply: JtsGeometry =
            geometry(value)


        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).add("value", value).toString

    }

    /**
      * * @param shape the shape to be transformed
      * [[GeoShape]] that returns the bounding box of a JTS geographical shape. The bounding box of shape is
      * the minimal rectangle containing the shape.
      */
    class BBox @JsonCreator() (@JsonProperty("shape") val shape: GeoShapes.GeoShape) extends GeoShapes.GeoShape {

        /**
          * Returns the bounding box of the specified [[JtsGeometry]].
          *
          * @return the convex hull
          */
        override def apply : JtsGeometry =
            CONTEXT.makeShape(CONTEXT.getGeometryFrom(shape.apply.getBoundingBox))


        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).toString
    }

    /**
      * @param shape       the shape to be transformed
      * @param minDistance the min allowed distance
      * @param maxDistance the max allowed distance
      *
      *                    [[GeoShape]] that returns the bounding shape of a JTS geographical shape.
      */
    class Buffer @JsonCreator() (@JsonProperty("shape") val shape : GeoShapes.GeoShape,
                                 @JsonProperty("min_distance") val minDistance : GeoDistance,
                                 @JsonProperty("max_distance") val maxDistance : GeoDistance) extends GeoShapes.GeoShape {

        /**
          * Returns the buffer of the specified [[JtsGeometry]].
          *
          * @return the buffer
          */
        override def apply :JtsGeometry = {
            val jts = shape.apply

            val max = if (maxDistance == null)
                        CONTEXT.makeShape(jts.getGeom)
                      else
                        jts.getBuffered(maxDistance.getDegrees, CONTEXT)

            if (minDistance != null) {
                val min = jts.getBuffered(minDistance.getDegrees, CONTEXT)
                val difference = max.getGeom.difference(min.getGeom)
                return CONTEXT.makeShape(difference)
            }
            max
        }

        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this)
              .add("minDistance", minDistance)
              .add("maxDistance", maxDistance)
              .toString

    }

    /**
      *  * @param shape the shape to be transformed
      * [[GeoShape]] that returns the center point of a JTS geographical shape.
      */
    class Centroid @JsonCreator() (@JsonProperty("shape") val shape: GeoShapes.GeoShape) extends GeoShapes.GeoShape {

        /**
          * Returns the center of the specified [[JtsGeometry]].
          *
          * @return the center
          */
        override def apply:JtsGeometry =
            CONTEXT.makeShape(shape.apply.getGeom.getCentroid)


        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).toString
    }

    /**
      * * @param shape the shape to be transformed
      * [[GeoShape]] that returns the convex hull of a JTS geographical shape.
      */
    class ConvexHull @JsonCreator() (@JsonProperty("shape") val shape: GeoShapes.GeoShape) extends GeoShapes.GeoShape {

        /**
          * Returns the convex hull of the specified [[JtsGeometry]].
          *
          * @return the convex hull
          */
        override def apply:JtsGeometry =
            CONTEXT.makeShape(shape.apply.getGeom.convexHull())


        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).toString

    }

    /**
      * @param shapes the shapes to be subtracted
      *
      * [[GeoShape]] that returns the difference of two JTS geographical shapes.
      */
    class Difference @JsonCreator() (@JsonProperty("shapes") val shapes : Array[GeoShapes.GeoShape]) extends GeoShapes.GeoShape {

        /**
          * Returns the difference of the specified shapes.
          *
          * @return the difference
          */
        override def apply:JtsGeometry = {
            var result = shapes.head.apply.getGeom
            for (shape : GeoShapes.GeoShape <- shapes) {
                result = result.difference(shape.apply.getGeom)
            }
            CONTEXT.makeShape(result)
        }

        /** @inheritdoc */
        override def toString : String =
            MoreObjects.toStringHelper(this).add("shapes", shapes).toString

    }

    /**
      *   * @param shapes the shapes to be intersected
      * [[GeoShape]] that returns the intersection of two JTS geographical shapes.
      */
    class Intersection @JsonCreator() (@JsonProperty("shapes") val shapes : Array[GeoShapes.GeoShape]) extends GeoShapes.GeoShape {

        /**
          * Returns the intersection of the specified shapes.
          *
          * @return the intersection
          */
        override def apply:JtsGeometry = {
            var  result = shapes.head.apply.getGeom
            for (shape : GeoShapes.GeoShape <-shapes ) {
                result = result.intersection(shape.apply.getGeom)
            }
            CONTEXT.makeShape(result)
        }

        /** @inheritdoc */
        override def toString: String =
            MoreObjects.toStringHelper(this).add("shapes", shapes).toString
    }

    /**
      * * @param shapes the shapes to be intersected
      * [[GeoShape]] that returns the union of two JTS geographical shapes.
      */
    class Union @JsonCreator() (@JsonProperty("shapes") val shapes : Array[GeoShapes.GeoShape]) extends GeoShapes.GeoShape {

        /**
          * Returns the intersection of the specified shapes.
          *
          * @return the intersection
          */
        override def apply:JtsGeometry = {
            var result = shapes.head.apply.getGeom
            for (shape : GeoShapes.GeoShape <- shapes) {
                result = result.union(shape.apply.getGeom)
            }
            CONTEXT.makeShape(result)
        }

        override def toString: String =
            MoreObjects.toStringHelper(this).add("shapes", shapes).toString
    }
}
