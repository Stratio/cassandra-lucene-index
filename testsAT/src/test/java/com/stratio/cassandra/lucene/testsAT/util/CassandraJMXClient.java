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
package com.stratio.cassandra.lucene.testsAT.util;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
class CassandraJMXClient {
    private JMXConnector jmxc;
    private JMXServiceURL url;

    CassandraJMXClient(String service) {
        try {
            url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + service + "/jmxrmi");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    void connect() {
        try {
            jmxc = JMXConnectorFactory.connect(url, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void disconnect() {
        try {
            jmxc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void invoke(String beanName, String operation, Object[] params, String[] signature)
    throws MalformedObjectNameException, IOException, MBeanException, InstanceNotFoundException, ReflectionException {
        jmxc.getMBeanServerConnection().invoke(new ObjectName(beanName), operation, params, signature);

    }

    public Object getAttribute(String s_name, String atribute)
    throws IOException, MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException,
           InstanceNotFoundException {

        ObjectName name = new ObjectName(s_name);
        return jmxc.getMBeanServerConnection().getAttribute(name, atribute);
    }
}
