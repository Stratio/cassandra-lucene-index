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

package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * A {@link Mapper} to map inet addresses.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class InetMapper extends KeywordMapper {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");

    private static final Pattern IPV6_COMPRESSED_PATTERN = Pattern.compile(
            "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$");

    /**
     * Builds a new {@link InetMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param indexed if the field supports searching
     * @param sorted if the field supports sorting
     * @param validated if the field must be validated
     */
    public InetMapper(String field, String column, Boolean indexed, Boolean sorted, Boolean validated) {
        super(field, column, indexed, sorted, validated, UTF8Type.instance, InetAddressType.instance);
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof InetAddress) {
            return ((InetAddress) value).getHostAddress();
        } else if (value instanceof String) {
            return doBase(name, (String) value);
        } else {
            throw new IndexException("Field '%s' requires an inet address, but found '%s'", name, value);
        }
    }

    private String doBase(String name, String value) {
        if (IPV4_PATTERN.matcher(value).matches() ||
            IPV6_PATTERN.matcher(value).matches() ||
            IPV6_COMPRESSED_PATTERN.matcher(value).matches()) {
            try {
                return InetAddress.getByName(value).getHostAddress();
            } catch (UnknownHostException e) {
                throw new IndexException(e, "Unknown host exception for field '%s' with value '%s'", name, value);
            }
        }
        throw new IndexException("Field '%s' with value '%s' can not be parsed as an inet address", name, value);
    }
}
