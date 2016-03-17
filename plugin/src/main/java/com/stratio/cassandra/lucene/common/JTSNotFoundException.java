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

package com.stratio.cassandra.lucene.common;

import com.stratio.cassandra.lucene.IndexException;

/**
 * {@link IndexException} to be thrown if <a href="http://www.vividsolutions.com/jts">Java Topology Suite (JTS)</a>
 * library is not found in classpath
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class JTSNotFoundException extends IndexException {

    static final String MESSAGE = "JTS JAR is not provided due to license compatibility issues, please include " +
                                  "jts-core-1.14.0.jar in Cassandra lib directory in order to use GeoShapeMapper or " +
                                  "GeoShapeCondition";

    /**
     * Default constructor.
     */
    public JTSNotFoundException() {
        super(MESSAGE);
    }
}
