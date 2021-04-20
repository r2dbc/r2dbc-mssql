/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A collection of {@link Encoded encoded parameters} for a single bind invocation of a prepared statement.
 * Bindings for Microsoft SQL Server are name-based and names are handled without the prefixing at-sign. The at sign is added during encoding.
 *
 * @author Mark Paluch
 */
class Binding {

    private final Map<String, RpcParameter> parameters = new LinkedHashMap<>();

    private boolean hasOutParameters = false;

    @Nullable
    private volatile String formalRepresentation;

    /**
     * Add a {@link Encoded encoded parameter} to the binding.
     *
     * @param name      the name of the {@link Encoded encoded parameter}
     * @param direction the direction of the encoded parameter
     * @param parameter the {@link Encoded encoded parameter}
     * @return this {@link Binding}
     */
    public Binding add(String name, RpcDirection direction, Encoded parameter) {

        Assert.requireNonNull(name, "Name must not be null");
        Assert.requireNonNull(direction, "RpcDirection must not be null");
        Assert.requireNonNull(parameter, "Parameter must not be null");

        this.formalRepresentation = null;
        this.parameters.put(name, new RpcParameter(direction, parameter));
        if (direction == RpcDirection.OUT) {
            this.hasOutParameters = true;
        }
        return this;
    }

    /**
     * Returns parameter names of the return values.
     *
     * @return
     */
    boolean hasOutParameters() {
        return this.hasOutParameters;
    }

    /**
     * Clear/release binding values.
     */
    void clear() {

        this.parameters.forEach((s, parameter) -> {
            while (parameter.encoded.refCnt() > 0) {
                parameter.encoded.release();
            }
        });

        this.parameters.clear();
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

        StringBuilder builder = new StringBuilder(this.parameters.size() * 16);
        Set<Map.Entry<String, RpcParameter>> entries = this.parameters.entrySet();

        for (Map.Entry<String, RpcParameter> entry : entries) {

            if (builder.length() != 0) {
                builder.append(',');
            }

            builder.append('@').append(entry.getKey()).append(' ').append(entry.getValue().encoded.getFormalType());

            if (entry.getValue().rpcDirection == RpcDirection.OUT) {
                builder.append(" OUTPUT");
            }
        }

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
    public void forEach(BiConsumer<String, RpcParameter> action) {

        Assert.requireNonNull(action, "Action must not be null");

        this.parameters.forEach(action);
    }

    Map<String, RpcParameter> getParameters() {
        return this.parameters;
    }

    /**
     * Returns whether this {@link Binding} is empty (i.e. no parameters bound).
     *
     * @return {@code true} if no parameters were bound.
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

    public static class RpcParameter {

        final RpcDirection rpcDirection;

        final Encoded encoded;

        public RpcParameter(RpcDirection rpcDirection, Encoded encoded) {
            this.rpcDirection = rpcDirection;
            this.encoded = encoded;
        }

    }

}
