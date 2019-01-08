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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * Identifier for an object, typically a type or a table name.
 *
 * @author Mark Paluch
 */
public final class Identifier {

    @Nullable
    private final String serverName;

    @Nullable
    private final String databaseName;

    @Nullable
    private final String schemaName;

    private final String objectName;

    private Identifier(@Nullable String serverName, @Nullable String databaseName, @Nullable String schemaName, String objectName) {

        this.serverName = serverName;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.objectName = Assert.requireNonNull(objectName, "Object name must not be null");
    }

    /**
     * Create a new {@link Identifier} given {@code objectName}.
     *
     * @param objectName the object name.
     * @return the {@link Identifier} for {@code objectName}.
     * @throws IllegalArgumentException when {@code objectName} is {@code null}.
     */
    public static Identifier objectName(String objectName) {
        return new Identifier(null, null, null, objectName);
    }

    /**
     * Creates a new {@link Builder} to build {@link Identifier}.
     *
     * @return a new {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Decode the identifier.
     *
     * @param buffer the data buffer.
     * @return the decoded {@link Identifier}.
     * @throws IllegalArgumentException when {@link ByteBuf} is {@code null}.
     */
    public static Identifier decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Buffer must not be null");

        // Multi-part names should have between 1 and 4 parts
        int parts = Decode.uByte(buffer);
        if (!(1 <= parts && parts <= 4)) {
            throw ProtocolException.invalidTds(String.format("Identifier must contain one to four parts, got: %d", parts));
        }

        // Each part is a length-prefixed Unicode string
        String[] nameParts = new String[parts];
        for (int i = 0; i < parts; i++) {
            nameParts[i] = Decode.unicodeUString(buffer);
        }

        String serverName = null;
        String databaseName = null;
        String schemaName = null;
        String objectName = nameParts[parts - 1];

        if (parts >= 2) {
            schemaName = nameParts[parts - 2];
        }

        if (parts >= 3) {
            databaseName = nameParts[parts - 3];
        }

        if (parts == 4) {
            serverName = nameParts[parts - 4];
        }

        return new Identifier(serverName, databaseName, schemaName, objectName);
    }

    @Nullable
    public String getServerName() {
        return this.serverName;
    }

    @Nullable
    public String getDatabaseName() {
        return this.databaseName;
    }

    @Nullable
    public String getSchemaName() {
        return this.schemaName;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public String asEscapedString() {

        StringBuilder fullName = new StringBuilder(256);

        if (StringUtils.hasText(this.serverName)) {
            fullName.append("[" + this.serverName + "].");
        }

        if (StringUtils.hasText(this.databaseName)) {
            fullName.append("[" + this.databaseName + "].");
        } else {
            Assert.state(StringUtils.isEmpty(this.serverName), "Server name must be empty");
        }

        if (StringUtils.hasText(this.schemaName)) {
            fullName.append("[" + this.schemaName + "].");
        } else if (StringUtils.hasText(this.databaseName)) {
            fullName.append('.');
        }

        fullName.append("[" + this.objectName + "]");

        return fullName.toString();
    }

    @Override
    public String toString() {
        return asEscapedString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Identifier)) {
            return false;
        }
        Identifier that = (Identifier) o;
        return Objects.equals(this.serverName, that.serverName) &&
            Objects.equals(this.databaseName, that.databaseName) &&
            Objects.equals(this.schemaName, that.schemaName) &&
            Objects.equals(this.objectName, that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.serverName, this.databaseName, this.schemaName, this.objectName);
    }

    /**
     * Builder for {@link Identifier}.
     */
    public static class Builder {

        @Nullable
        private String serverName;

        @Nullable
        private String databaseName;

        @Nullable
        private String schemaName;

        @Nullable
        private String objectName;

        private Builder() {
        }

        /**
         * Configure an {@code objectName}.
         *
         * @param objectName the object name, must not be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code objectName} is {@code null}.
         */
        public Builder objectName(String objectName) {

            this.objectName = Assert.requireNonNull(objectName, "Object name must not be null");

            return this;
        }

        /**
         * Configure a {@code schemaName}.
         *
         * @param schemaName the schema name, must not be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code schemaName} is {@code null}.
         */
        public Builder schemaName(String schemaName) {

            this.schemaName = Assert.requireNonNull(schemaName, "Schema name must not be null");

            return this;
        }

        /**
         * Configure a {@code databaseName}.
         *
         * @param databaseName the database name, must not be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link ByteBuf} is {@code null}.
         */
        public Builder databaseName(String databaseName) {

            this.databaseName = Assert.requireNonNull(databaseName, "Database name must not be null");

            return this;
        }

        /**
         * Configure a {@code serverName}. Requires the {@link #databaseName(String)} be set if the server name is not empty.
         *
         * @param serverName the server name, must not be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code serverName} is {@code null}.
         */
        public Builder serverName(String serverName) {

            this.serverName = Assert.requireNonNull(serverName, "Server name must not be null");

            return this;
        }

        /**
         * Build the {@link Identifier}.
         *
         * @return the {@link Identifier}
         */
        public Identifier build() {

            Assert.notNull(this.objectName, "Object name must not be null");

            Assert.state(StringUtils.isEmpty(this.serverName) || !StringUtils.isEmpty(this.databaseName), "Server name must be either null or both, server name and database name must " +
                "be provided");

            return new Identifier(this.serverName, this.databaseName, this.schemaName, this.objectName);
        }
    }
}
