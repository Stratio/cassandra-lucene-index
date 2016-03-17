/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;

/**
 * Utilities for geospatial related stuff.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeospatialUtils {

    /** The default max number of levels for geohash search trees. */
    public static final int DEFAULT_GEOHASH_MAX_LEVELS = 11;

    /** The min accepted longitude. */
    public static final double MIN_LATITUDE = -90.0;

    /** The max accepted longitude. */
    public static final double MAX_LATITUDE = 90.0;

    /** The min accepted longitude. */
    public static final double MIN_LONGITUDE = -180.0;

    /** The max accepted longitude. */
    public static final double MAX_LONGITUDE = 180.0;

    /**
     * Checks if the specified max levels is correct.
     *
     * @param maxLevels the maximum number of levels in the tree
     * @return the validated max levels
     */
    public static int validateGeohashMaxLevels(Integer maxLevels) {
        if (maxLevels == null) {
            return DEFAULT_GEOHASH_MAX_LEVELS;
        } else if (maxLevels < 1 || maxLevels > GeohashPrefixTree.getMaxLevelsPossible()) {
            throw new IndexException("max_levels must be in range [1, %s], but found %s",
                                     GeohashPrefixTree.getMaxLevelsPossible(),
                                     maxLevels);
        }
        return maxLevels;
    }

    /**
     * Checks if the specified latitude is correct.
     *
     * @param name the name of the latitude field
     * @param latitude the value of the latitude field
     * @return the latitude
     */
    public static Double checkLatitude(String name, Double latitude) {
        if (latitude == null) {
            throw new IndexException("%s required", name);
        } else if (latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new IndexException("%s must be in range [%s, %s], but found %s",
                                     name,
                                     MIN_LATITUDE,
                                     MAX_LATITUDE,
                                     latitude);
        }
        return latitude;
    }

    /**
     * Checks if the specified longitude is correct.
     *
     * @param name the name of the longitude field
     * @param longitude the value of the longitude field
     * @return the longitude
     */
    public static Double checkLongitude(String name, Double longitude) {
        if (longitude == null) {
            throw new IndexException("%s required", name);
        } else if (longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new IndexException("%s must be in range [%s, %s], but found %s",
                                     name,
                                     MIN_LONGITUDE,
                                     MAX_LONGITUDE,
                                     longitude);
        }
        return longitude;
    }
}
