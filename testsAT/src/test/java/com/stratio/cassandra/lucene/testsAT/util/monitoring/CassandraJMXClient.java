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

import com.datastax.driver.core.exceptions.InvalidQueryException;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class CassandraJMXClient implements CassandraMonitoringClient {

    private JMXConnector jmxc;
    private JMXServiceURL url;

    public CassandraJMXClient(String service) {
        try {
            url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + service + "/jmxrmi");
        } catch (MalformedURLException e) {
            throw new RuntimeException("Error while creating JMX client", e);
        }
    }

    public CassandraJMXClient connect() {
        try {
            jmxc = JMXConnectorFactory.connect(url, null);
        } catch (IOException e) {
            throw new RuntimeException("Error while connecting JMX client", e);
        }
        return this;
    }

    public void disconnect() {
        try {
            jmxc.close();
            jmxc = null;
        } catch (IOException e) {
            throw new RuntimeException("Error while disconnecting JMX client", e);
        }
    }

    public void invoke(String beanName, String operation, Object[] params) throws RuntimeException {
        String[] signature= new String[params.length];
        for (int i=0;i<params.length;i++) {
            signature[i]=params[i].getClass().getName();

        }
        try {
            jmxc.getMBeanServerConnection().invoke(new ObjectName(beanName), operation, params, signature);
        } catch (Exception e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }

    public Object getAttribute(String s_name, String attribute) throws RuntimeException {
        try {
            ObjectName name = new ObjectName(s_name);
            return jmxc.getMBeanServerConnection().getAttribute(name, attribute);
        } catch (Exception e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }

}
