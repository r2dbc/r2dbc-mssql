/*
 * Copyright 2018-2020 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * Parameter context for RPC parameters. Encapsulated {@link RpcDirection} and an optional {@link ValueContext}.
 *
 * @author Mark Paluch
 */
public final class RpcParameterContext {

    private static final RpcParameterContext IN = new RpcParameterContext(RpcDirection.IN, null, null);

    private static final RpcParameterContext OUT = new RpcParameterContext(RpcDirection.OUT, null, null);

    private final RpcDirection direction;

    @Nullable
    private final ValueContext valueContext;

    @Nullable
    private final SqlServerType serverType;

    private RpcParameterContext(RpcDirection direction, @Nullable ValueContext valueContext, @Nullable SqlServerType serverType) {
        this.direction = direction;
        this.serverType = serverType;
        this.valueContext = valueContext;
    }

    /**
     * Returns a context for in (client to server) parameters.
     *
     * @return a context for in (client to server) parameters.
     * @see RpcDirection#IN
     */
    public static RpcParameterContext in() {
        return IN;
    }

    /**
     * Returns a context for in (client to server) parameters with the associated {@link ValueContext}.
     *
     * @param valueContext the value context.
     * @return a context for in (client to server) parameters with the associated {@link ValueContext}.
     * @see RpcDirection#IN
     */
    public static RpcParameterContext in(ValueContext valueContext) {
        return new RpcParameterContext(RpcDirection.IN, Assert.requireNonNull(valueContext, "ValueContext must not be null"), null);
    }

    /**
     * Returns a context for out (server to client) parameters.
     *
     * @return a context for out (server to client) parameters.
     * @see RpcDirection#OUT
     */
    public static RpcParameterContext out() {
        return OUT;
    }

    /**
     * Returns a context for out (server to client) parameters with the associated {@link ValueContext}.
     *
     * @param valueContext the value context.
     * @return a context for out (server to client) parameters with the associated {@link ValueContext}.
     * @see RpcDirection#IN
     */
    public static RpcParameterContext out(ValueContext valueContext) {
        return new RpcParameterContext(RpcDirection.OUT, Assert.requireNonNull(valueContext, "ValueContext must not be null"), null);
    }

    /**
     * @return the RPC direction.
     */
    public RpcDirection getDirection() {
        return this.direction;
    }

    /**
     * @return {@code true} if this parameter is a in parameter.
     */
    public boolean isIn() {
        return this.direction == RpcDirection.IN;
    }

    /**
     * @return {@code true} if this parameter is a out parameter.
     */
    public boolean isOut() {
        return this.direction == RpcDirection.OUT;
    }

    /**
     * @return the value context, can be {@code null}.
     */
    @Nullable
    public ValueContext getValueContext() {
        return this.valueContext;
    }

    @Nullable
    public SqlServerType getServerType() {
        return this.serverType;
    }

    /**
     * Return the required {@link ValueContext} or throw {@link IllegalStateException} if no {@link ValueContext} is set..
     *
     * @return the value context.
     * @throws IllegalArgumentException if the {@link ValueContext} is not set.
     */
    public ValueContext getRequiredValueContext() {

        ValueContext valueContext = getValueContext();

        if (valueContext == null) {
            throw new IllegalStateException("No ValueContext set");
        }

        return valueContext;
    }

    /**
     * Return the required {@link ValueContext} as typed {@code T} or throw {@link IllegalStateException} if no {@link ValueContext} is set.
     *
     * @return the value context.
     * @throws IllegalArgumentException if the {@link ValueContext} is not set.
     * @throws ClassCastException       if the {@link ValueContext} is not of type {@code contextType}.
     */
    public <T extends ValueContext> T getRequiredValueContext(Class<? extends T> contextType) {

        ValueContext valueContext = getRequiredValueContext();

        return contextType.cast(valueContext);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [direction=").append(this.direction);
        sb.append(", valueContext=").append(this.valueContext);
        sb.append(']');
        return sb.toString();
    }

    public RpcParameterContext withServerType(SqlServerType serverType) {
        Assert.requireNonNull(serverType, "SqlServerType must not be null");
        return new RpcParameterContext(this.direction, this.valueContext, serverType);
    }

    /**
     * Marker interface for additional contextual information that are used for value encoding.
     */
    public interface ValueContext {

        public static ValueContext character(Collation collation, boolean sendStringParametersAsUnicode) {
            return new CharacterValueContext(collation, sendStringParametersAsUnicode);
        }

    }

    /**
     * Marker interface for additional contextual information.
     */
    public static class CharacterValueContext implements ValueContext {

        private final Collation collation;

        private final boolean sendStringParametersAsUnicode;

        public CharacterValueContext(Collation collation, boolean sendStringParametersAsUnicode) {
            this.collation = collation;
            this.sendStringParametersAsUnicode = sendStringParametersAsUnicode;
        }

        public Collation getCollation() {
            return this.collation;
        }

        public boolean isSendStringParametersAsUnicode() {
            return this.sendStringParametersAsUnicode;
        }

    }

}
