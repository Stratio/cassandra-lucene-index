/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.geospatial.GeoBBoxCondition;
import com.stratio.cassandra.lucene.geospatial.GeoDistanceCondition;
import com.stratio.cassandra.lucene.geospatial.GeoDistanceRangeCondition;
import com.stratio.cassandra.lucene.geospatial.GeoShapeCondition;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.analysis.AnalysisUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * The abstract base class for queries.
 * <p/>
 * Known subclasses are: <ul> <li> {@link BooleanCondition} <li> {@link ContainsCondition} <li> {@link FuzzyCondition}
 * <li> {@link MatchCondition} <li> {@link PhraseCondition} <li> {@link PrefixCondition} <li> {@link RangeCondition}
 * <li> {@link WildcardCondition} <li> {@link GeoShapeCondition} <li> {@link GeoDistanceCondition} <li> {@link
 * GeoDistanceRangeCondition} <li> {@link GeoBBoxCondition} </ul>
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = BooleanCondition.class, name = "boolean"),
               @JsonSubTypes.Type(value = ContainsCondition.class, name = "contains"),
               @JsonSubTypes.Type(value = FuzzyCondition.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = LuceneCondition.class, name = "lucene"),
               @JsonSubTypes.Type(value = MatchCondition.class, name = "match"),
               @JsonSubTypes.Type(value = MatchAllCondition.class, name = "match_all"),
               @JsonSubTypes.Type(value = RangeCondition.class, name = "range"),
               @JsonSubTypes.Type(value = PhraseCondition.class, name = "phrase"),
               @JsonSubTypes.Type(value = PrefixCondition.class, name = "prefix"),
               @JsonSubTypes.Type(value = RegexpCondition.class, name = "regexp"),
               @JsonSubTypes.Type(value = WildcardCondition.class, name = "wildcard"),
               @JsonSubTypes.Type(value = GeoShapeCondition.class, name = "geo_shape"),
               @JsonSubTypes.Type(value = GeoDistanceCondition.class, name = "geo_distance"),
               @JsonSubTypes.Type(value = GeoDistanceRangeCondition.class, name = "geo_distance_range"),
               @JsonSubTypes.Type(value = GeoBBoxCondition.class, name = "geo_bounding_box"),})
public abstract class Condition {

    /** The default boost to be used. */
    public static final float DEFAULT_BOOST = 1.0f;

    /** The boost to be used. */
    @JsonProperty("boost")
    protected final float boost;

    /**
     * Abstract {@link Condition} builder receiving the boost to be used.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}.
     */
    @JsonCreator
    public Condition(@JsonProperty("boost") Float boost) {
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    public float getBoost() {
        return boost;
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param schema The schema to be used.
     * @return The Lucene {@link Query} representation of this condition.
     */
    public abstract Query query(Schema schema);

    /**
     * Returns the Lucene {@link Filter} representation of this condition.
     *
     * @param schema The schema to be used.
     * @return The Lucene {@link Filter} representation of this condition.
     */
    public Filter filter(Schema schema) {
        return new QueryWrapperFilter(query(schema));
    }

    protected String analyze(String field, String value, Schema schema) {
        Analyzer analyzer = schema.getAnalyzer();
        return AnalysisUtils.instance.analyzeAsText(field, value, analyzer);
    }

}
