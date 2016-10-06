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
package com.stratio.cassandra.lucene.codecs;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MultiSorter;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;


public abstract class SerializableSortField extends SortField {
    public SerializableSortField() {
        this(null, Type.CUSTOM);
    }
    public SerializableSortField(String field, Type type) {super(field,type);}
    public SerializableSortField(String field, FieldComparatorSource comparator) {
        super(field, comparator);
    }

    public abstract SerializableSortField read(DataInput input) throws IOException;
    public abstract void write(DataOutput output) throws IOException;
    public abstract MultiSorter.CrossReaderComparator getCrossReaderComparator(List<CodecReader> readers) throws IOException;
}