/**
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
package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;

/**
 * Utilities for geospatial related stuff.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeospatialUtils {

    public static final int DEFAULT_GEOHASH_MAX_LEVELS = 11;

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
}
