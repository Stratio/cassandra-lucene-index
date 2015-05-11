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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.Condition;

/**
 * Class for building new {@link Condition}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ConditionBuilder<T extends Condition, K extends ConditionBuilder<T, K>> implements Builder<T> {

    /** The boost for the {@link Condition} to be built. */
    protected Float boost;

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     * Condition#DEFAULT_BOOST}
     *
     * @param boost The boost for the {@link Condition} to be built.
     * @return This builder with the specified boost.
     */
    @SuppressWarnings("unchecked")
    public K boost(float boost) {
        this.boost = boost;
        return (K) this;
    }

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     * Condition#DEFAULT_BOOST}
     *
     * @param boost The boost for the {@link Condition} to be built.
     * @return This builder with the specified boost.
     */
    @SuppressWarnings("unchecked")
    public K boost(Number boost) {
        this.boost = boost == null ? null : boost.floatValue();
        return (K) this;
    }

    /**
     * Returns the {@link Condition} represented by this builder.
     *
     * @return The {@link Condition} represented by this builder.
     */
    @Override
    public abstract T build();
}
