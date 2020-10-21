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

package io.r2dbc.mssql.message.header;

import io.r2dbc.mssql.util.Assert;

/**
 * Base header options defining {@link Type} and {@link Status}. Typically used to provide a TDS packet context so
 * lower-level components can form a {@link Header} packet from this options and TDS payload.
 */
public interface HeaderOptions {

    /**
     * Create a {@link HeaderOptions} object with {@link Status.StatusBit} set.
     *
     * @param bit status bit to set.
     * @return the {@link HeaderOptions}.
     */
    default HeaderOptions and(Status.StatusBit bit) {

        Status status = getStatus();
        Status newStatus = status.and(bit);

        if (status == newStatus) {
            return this;
        }

        return DefaultHeaderOptions.get(getType(), newStatus);
    }

    /**
     * Create a {@link HeaderOptions} object with {@link Status.StatusBit} removed.
     *
     * @param bit status bit to remove.
     * @return the {@link HeaderOptions}.
     */
    default HeaderOptions not(Status.StatusBit bit) {

        Status status = getStatus();
        Status newStatus = status.not(bit);

        if (status == newStatus) {
            return this;
        }

        return DefaultHeaderOptions.get(getType(), newStatus);
    }

    /**
     * Defines the type of message. 1-byte.
     *
     * @return the message type.
     */
    Type getType();

    /**
     * Status is a bit field used to indicate the message state. 1-byte.
     *
     * @return the {@link Status.StatusBit}.
     */
    Status getStatus();

    /**
     * Check if the header status has set the {@link Status.StatusBit}.
     *
     * @param bit the status bit.
     * @return {@code true} of the bit is set; {@code false} otherwise.
     */
    boolean is(Status.StatusBit bit);

    /**
     * Create {@link HeaderOptions} given {@link Type} and {@link Status}.
     *
     * @param type   the header {@link Type}.
     * @param status the {@link Status}.
     * @return the {@link HeaderOptions}.
     */
    static HeaderOptions create(Type type, Status status) {

        Assert.requireNonNull(type, "Type must not be null");
        Assert.requireNonNull(status, "Status must not be null");

        return new DefaultHeaderOptions(type, status);
    }

}
