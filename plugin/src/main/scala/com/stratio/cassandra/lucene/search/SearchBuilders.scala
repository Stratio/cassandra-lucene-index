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
package com.stratio.cassandra.lucene.search

import com.stratio.cassandra.lucene.common.{GeoDistance, GeoShapes}
import com.stratio.cassandra.lucene.search.condition.builder._
import com.stratio.cassandra.lucene.search.sort.builder.{GeoDistanceSortFieldBuilder, SimpleSortFieldBuilder, SortFieldBuilder}

/**
 * Factory for [[SearchBuilder]] and [[com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder]]s.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
object SearchBuilders {

    /**
     * Returns a new [[SearchBuilder]].
     *
     * @return the search builder
     */
    def search() : SearchBuilder = new SearchBuilder()

    /**
     * Returns a new [[SearchBuilder]] using the specified filtering [[com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder]]s to not be used in
     * scoring.
     *
     * @param filters the condition builders to be used as filter
     * @return a new [[SearchBuilder]]
     */
    def  filter(filters : Array[ConditionBuilder[_,_]]): SearchBuilder =
        SearchBuilders.search().filter(filters)

    def  filter(filter : ConditionBuilder[_,_]): SearchBuilder =
        SearchBuilders.search().filter(filter)

    /**
     * Returns a new [[SearchBuilder]] using the specified querying [[ConditionBuilder]]s o be used in scoring.
     *
     * @param queries the condition builders to be used as query
     * @return a new [[SearchBuilder]]
     */
    def query(queries : Array[ConditionBuilder[_,_]]) : SearchBuilder = search().query(queries)

    def query(query : ConditionBuilder[_,_]) : SearchBuilder = search().query(query)


    /**
     * Returns a new [[SearchBuilder]] using the specified [[SortFieldBuilder]]s as sorting.
     *
     * @param sortFields the sorting builder
     * @return a new [[SearchBuilder]] with the specified sort
     */
    def sort(sortFields : Array[SortFieldBuilder[_,_]]) : SearchBuilder = search().sort(sortFields)

    def sort(sortFields : SortFieldBuilder[_,_]) : SearchBuilder = search().sort(sortFields)


    /**
     * Returns a new [[SearchBuilder]] using the specified index refresh option.
     *
     * @param refresh if the search to be built should refresh the index
     * @return a new [[SearchBuilder]] with the specified sort
     */
    def refresh(refresh : Boolean ) : SearchBuilder = search().refresh(refresh)


    /**
     * Returns a new [[com.stratio.cassandra.lucene.search.condition.builder.BooleanConditionBuilder]].
     *
     * @return a new boolean condition builder
     */
    def bool : BooleanConditionBuilder = new BooleanConditionBuilder

    /**
     * Returns a new [[AllConditionBuilder]] for the specified field and value.
     *
     * @return a new all condition builder
     */
    def all : AllConditionBuilder = new AllConditionBuilder


    /**
     * Returns a new [[com.stratio.cassandra.lucene.search.condition.builder.FuzzyConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new fuzzy condition builder
     */
    def fuzzy(field : String ,value : String) : FuzzyConditionBuilder = new FuzzyConditionBuilder(field, value)


    /**
     * Returns a new [[LuceneConditionBuilder]] with the specified query.
     *
     * @param query the Lucene syntax query
     * @return a new Lucene condition builder
     */
    def lucene(query : String ) : LuceneConditionBuilder = new LuceneConditionBuilder(query)

    /**
     * Returns a new [[MatchConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new match condition builder
     */
    def `match`(field: String,value: Any) : MatchConditionBuilder = new MatchConditionBuilder(field, value)

    /**
     * Returns a new [[ContainsConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     * @return a new match condition builder
     */
    def contains(field : String , values: Array[Any]) : ContainsConditionBuilder = new ContainsConditionBuilder(field, values)


    /**
     * Returns a new [[NoneConditionBuilder]] for the specified field and value.
     *
     * @return a new none condition builder
     */
    def none : NoneConditionBuilder = new NoneConditionBuilder()


    /**
     * Returns a new [[PhraseConditionBuilder]] for the specified field and values.
     *
     * @param field the name of the field to be matched
     * @param value The text to be matched.
     * @return a new phrase condition builder
     */
    def phrase(field : String , value : String) : PhraseConditionBuilder = new PhraseConditionBuilder(field, value)


    /**
     * Returns a new [[PrefixConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new prefix condition builder
     */
    def prefix(field : String, value : String ) : PrefixConditionBuilder = new PrefixConditionBuilder(field, value)

    /**
     * Returns a new [[RangeConditionBuilder]] for the specified field.
     *
     * @param field the name of the field to be matched
     * @return a new range condition builder
     */
    def range(field: String) : RangeConditionBuilder =new RangeConditionBuilder(field)

    /**
     * Returns a new [[RegexpConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new regexp condition builder
     */
    def regexp(field : String, value: String ) : RegexpConditionBuilder = new RegexpConditionBuilder(field, value)

    /**
     * Returns a new [[WildcardConditionBuilder]] for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new wildcard condition builder
     */
    def wildcard(field: String ,value: String) : WildcardConditionBuilder = new WildcardConditionBuilder(field, value)


    /**
     * Returns a new [[GeoBBoxConditionBuilder]] with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     * @param minLatitude The minimum accepted latitude.
     * @param maxLatitude The maximum accepted latitude.
     * @return a new geo bounding box condition builder
     */
    def  geoBBox(   field : String,
                    minLongitude: Double,
                    maxLongitude: Double,
                    minLatitude: Double,
                    maxLatitude: Double) : GeoBBoxConditionBuilder =
        new GeoBBoxConditionBuilder(field, minLatitude, maxLatitude, minLongitude, maxLongitude)


    /**
     * Returns a new [[GeoDistanceConditionBuilder]] with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @param longitude The longitude of the reference point.
     * @param latitude The latitude of the reference point.
     * @param maxDistance The max allowed distance.
     * @return a new geo distance condition builder
     */
    def geoDistance(    field : String,
                        longitude : Double,
                        latitude: Double ,
                        maxDistance: String) : GeoDistanceConditionBuilder =
        new GeoDistanceConditionBuilder(field, latitude, longitude, GeoDistance.parse(maxDistance))


    /**
     * Returns a new [[GeoShapeConditionBuilder]] with the specified field reference point.
     *
     * Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape
     * @return a new geo shape condition builder
     */
    def geoShape(field : String ,shape :  GeoShapes.GeoShape) : GeoShapeConditionBuilder  =new GeoShapeConditionBuilder(field, shape)

    /**
     * Returns a new [[GeoShapeConditionBuilder]] with the specified field reference point.
     *
     * Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape in <a href="http://en.wikipedia.org/wiki/Well-known_text"> WKT</a> format
     * @return a new geo shape condition builder
     */
    def geoShape(field : String ,shape:  String ) : GeoShapeConditionBuilder = new GeoShapeConditionBuilder(field, new GeoShapes.WKT(shape));

    /**
     * Returns a new [[DateRangeConditionBuilder]] with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @return a new date range condition builder
     */
    def dateRange(field : String ) : DateRangeConditionBuilder = new DateRangeConditionBuilder(field)

    /**
     * Returns a new [[SimpleSortFieldBuilder]] for the specified field.
     *
     * @param field the name of the field to be sorted by
     * @return a new simple sort field condition builder
     */
    def field(field: String ) : SimpleSortFieldBuilder = new SimpleSortFieldBuilder(field)

    /**
     * Returns a new [[GeoDistanceSortFieldBuilder]] for the specified field.
     *
     * @param mapper the name of mapper to use to calculate distance
     * @param latitude the latitude of the reference point
     * @param longitude the longitude of the reference point
     * @return a new geo distance sort field builder
     */
    def geoDistance(mapper: String, latitude : Double, longitude : Double) : GeoDistanceSortFieldBuilder = new GeoDistanceSortFieldBuilder(mapper, latitude, longitude);

    /**
     * Returns a new [[BitemporalConditionBuilder]] for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new bitemporal condition builder for the specified field
     */
    def bitemporal(field: String ) : BitemporalConditionBuilder = new BitemporalConditionBuilder(field)

}
