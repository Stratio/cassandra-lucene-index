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
package com.stratio.cassandra.lucene.testsAT.suite;

import com.stratio.cassandra.lucene.testsAT.geospatial.GeoBBoxSearchAT;
import com.stratio.cassandra.lucene.testsAT.geospatial.GeoShapeSearchOverGeoPointsAT;
import com.stratio.cassandra.lucene.testsAT.geospatial.GeoShapeSearchOverGeoShapesAT;
import com.stratio.cassandra.lucene.testsAT.geospatial.GeoShapeSearchSpatialOperationsAT;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({GeoBBoxSearchAT.class,
               GeoShapeSearchOverGeoShapesAT.class,
               GeoShapeSearchOverGeoPointsAT.class,
               GeoShapeSearchSpatialOperationsAT.class})
public class GeospatialSuite {
}
