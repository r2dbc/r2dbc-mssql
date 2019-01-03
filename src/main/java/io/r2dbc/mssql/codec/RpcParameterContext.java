/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * Parameter context for RPC parameters. Encapsulated {@link RpcDirection} and an optional {@link Collation}.
 *
 * @author Mark Paluch
 */
public final class RpcParameterContext {

    private static final RpcParameterContext IN = new RpcParameterContext(RpcDirection.IN, null);

    private static final RpcParameterContext OUT = new RpcParameterContext(RpcDirection.OUT, null);

    private final RpcDirection direction;

    @Nullable
    private final Collation collation;

    private RpcParameterContext(RpcDirection direction, @Nullable Collation collation) {
        this.direction = direction;
        this.collation = collation;
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
     * Returns a context for in (client to server) parameters with the associated {@link Collation}.
     *
     * @param collation the collation for character values.
     * @return a context for in (client to server) parameters with the associated {@link Collation}.
     * @see RpcDirection#IN
     */
    public static RpcParameterContext in(Collation collation) {
        return new RpcParameterContext(RpcDirection.IN, Assert.requireNonNull(collation, "Collation must not be null"));
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
     * Returns a context for out (server to client) parameters with the associated {@link Collation}.
     *
     * @param collation the collation for character values.
     * @return a context for out (server to client) parameters with the associated {@link Collation}.
     * @see RpcDirection#IN
     */
    public static RpcParameterContext out(Collation collation) {
        return new RpcParameterContext(RpcDirection.OUT, Assert.requireNonNull(collation, "Collation must not be null"));
    }

    /**
     * @return the RPC direction.
     */
    public RpcDirection getDirection() {
        return this.direction;
    }

    /**
     * @return {@literal true} if this parameter is a in parameter.
     */
    public boolean isIn() {
        return this.direction == RpcDirection.IN;
    }

    /**
     * @return {@literal true} if this parameter is a out parameter.
     */
    public boolean isOut() {
        return this.direction == RpcDirection.OUT;
    }

    /**
     * @return the collation, can be {@code null}.
     */
    @Nullable
    public Collation getCollation() {
        return this.collation;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [direction=").append(this.direction);
        sb.append(", collation=").append(this.collation);
        sb.append(']');
        return sb.toString();
    }
}
