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
package com.stratio.cassandra.lucene.schema.mapping;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * A {@link ColumnMapper} to map inet addresses.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperInet extends ColumnMapperKeyword {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");

    private static final Pattern IPV6_COMPRESSED_PATTERN = Pattern.compile(
            "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$");

    /**
     * Builds a new {@link ColumnMapperInet}.
     *
     * @param name    The name of the mapper.
     * @param indexed If the field supports searching.
     * @param sorted  If the field supports sorting.
     */
    public ColumnMapperInet(String name, Boolean indexed, Boolean sorted) {
        super(name, indexed, sorted, AsciiType.instance, UTF8Type.instance, InetAddressType.instance);
    }

    /** {@inheritDoc} */
    @Override
    public String base(String name, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof InetAddress) {
            InetAddress inetAddress = (InetAddress) value;
            return inetAddress.getHostAddress();
        } else if (value instanceof String) {
            String svalue = (String) value;
            if (IPV4_PATTERN.matcher(svalue).matches() ||
                IPV6_PATTERN.matcher(svalue).matches() ||
                IPV6_COMPRESSED_PATTERN.matcher(svalue).matches()) {
                try {
                    return InetAddress.getByName(svalue).getHostAddress();
                } catch (UnknownHostException e) {
                    Log.error(e, e.getMessage());
                }
            }
        }
        return error("Field '%s' requires an inet address, but found '%s'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("indexed", indexed).add("sorted", sorted).toString();
    }
}
