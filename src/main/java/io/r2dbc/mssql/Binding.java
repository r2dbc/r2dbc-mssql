/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A collection of {@link Encoded encoded parameters} for a single bind invocation of a prepared statement.
 * Bindings for Microsoft SQL Server are name-based and names are handled without the prefixing at-sign. The at sign is added during encoding.
 *
 * @author Mark Paluch
 */
class Binding {

    private final Map<String, Encoded> parameters = new LinkedHashMap<>();

    @Nullable
    private volatile String formalRepresentation;

    /**
     * Add a {@link Encoded encoded parameter} to the binding.
     *
     * @param name      the name of the {@link Encoded encoded parameter}
     * @param parameter the {@link Encoded encoded parameter}
     * @return this {@link Binding}
     */
    public Binding add(String name, Encoded parameter) {

        Assert.requireNonNull(name, "Name must not be null");
        Assert.requireNonNull(parameter, "Parameter must not be null");

        this.formalRepresentation = null;
        this.parameters.put(name, parameter);

        return this;
    }

    /**
     * Returns a formal representation of the bound parameters such as {@literal @P0 VARCHAR(8000), @P1 DECIMAL(12,6)}
     *
     * @return a formal representation of the bound parameters.
     */
    public String getFormalParameters() {

        String formalRepresentation = this.formalRepresentation;
        if (formalRepresentation != null) {
            return formalRepresentation;
        }

        StringBuilder builder = new StringBuilder();
        this.parameters.forEach((name, encoded) -> {

            if (builder.length() != 0) {
                builder.append(',');
            }

            builder.append('@').append(name).append(' ').append(encoded.getFormalType());

        });

        formalRepresentation = builder.toString();
        this.formalRepresentation = formalRepresentation;

        return formalRepresentation;
    }

    /**
     * Performs the given action for each entry in this binding until all bound parameters
     * have been processed or the action throws an exception.   Unless
     * otherwise specified by the implementing class, actions are performed in
     * the order of entry set iteration (if an iteration order is specified.)
     *
     * @param action The action to be performed for each bound parameter.
     */
    public void forEach(BiConsumer<String, Encoded> action) {

        Assert.requireNonNull(action, "Action must not be null");

        this.parameters.forEach(action);
    }

    public Map<String, Encoded> getParameters() {
        return this.parameters;
    }

    /**
     * Returns whether this {@link Binding} is empty (i.e. no parameters bound).
     *
     * @return {@literal true} if no parameters were bound.
     */
    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

    /**
     * Returns the number of bound parameters.
     *
     * @return the number of bound parameters.
     */
    int size() {
        return this.parameters.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Binding that = (Binding) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [parameters=").append(this.parameters);
        sb.append(']');
        return sb.toString();
    }
}
