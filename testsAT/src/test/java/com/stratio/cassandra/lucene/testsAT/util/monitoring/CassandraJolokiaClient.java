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

import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.request.J4pExecRequest;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pReadResponse;
import org.jolokia.client.request.J4pResponse;

import javax.management.MalformedObjectNameException;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class CassandraJolokiaClient implements CassandraMonitoringClient {
    private J4pClient j4pClient;
    private String service;
    public CassandraJolokiaClient(String service) {
        this.service=service;
    }

    public CassandraJolokiaClient connect() {
        j4pClient = new J4pClient("http://"+service+"/jolokia");
        return this;
    }

    public void disconnect() {
        this.j4pClient=null;
    }

    public Object read(String s_name, String attribute) throws RuntimeException {
        System.out.println("calling getAtribute with s_name: "+s_name+" atribute: "+attribute);
        J4pReadRequest req = null;
        try {
            req = new J4pReadRequest(s_name, attribute);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
            throw new RuntimeException("MalformedObjectNameException: "+e.getMessage());
        }
        try {
            J4pReadResponse response= j4pClient.execute(req);
            return response.getValue();
        } catch (J4pException e) {
            e.printStackTrace();
            throw new RuntimeException("J4pException: "+e.getMessage());
        }

    }

    @Override
    public void invokeMEthod(String beanName, String operation, Object[] params) throws RuntimeException {
        String[] signature= new String[params.length];
        for (int i=0;i<params.length;i++) {
            signature[i]=params[i].getClass().getName();
        }
        try {
            J4pExecRequest exec = new J4pExecRequest(beanName, operation, params);
            j4pClient.execute(exec);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        }

    }
}
