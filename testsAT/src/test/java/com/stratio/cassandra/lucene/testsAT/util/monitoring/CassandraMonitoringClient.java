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
package com.stratio.cassandra.lucene.testsAT.util.monitoring;

/**
 * Client for consuming the JMX monitoring services of a Cassandra node.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public interface CassandraMonitoringClient {

    /**
     * Connects to the node.
     *
     * @return this
     */
    CassandraMonitoringClient connect();

    /**
     * Closes the connection to the node.
     */
    void disconnect();

    /**
     * Gets the value of a specific attribute of a named MBean.
     *
     * @param bean the name of the MBean from which the attribute is to be retrieved
     * @param attribute the name of the attribute to be retrieved
     * @return the value of the retrieved attribute
     */
    Object read(String bean, String attribute);

    /**
     * Invokes an operation on an MBean.
     *
     * @param bean the name of the MBean from which the attribute is to be retrieved
     * @param operation the name of the operation to be invoked
     * @param params n array containing the parameters to be set when the operation is invoked
     * @param signature An array containing the signature of the operation, an array of class names in the format
     * returned by {@link Class#getName()}.
     */
    void invoke(String bean, String operation, Object[] params, String[] signature);
}
