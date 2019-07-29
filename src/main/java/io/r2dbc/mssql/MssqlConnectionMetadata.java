/*
 * Copyright 2019 the original author or authors.
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

/**
 * Connection metadata for a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
public final class MssqlConnectionMetadata {

    private final String databaseVersion;

    public MssqlConnectionMetadata(String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    /**
     * Retrieves the name of this database product.
     *
     * @return database product name
     */
    public String getDatabaseVersion() {
        return this.databaseVersion;
    }

    /**
     * Retrieves the name of this database product.
     *
     * @return database product name
     */
    public String getDatabaseProductName() {
        return MssqlConnectionFactoryMetadata.NAME;
    }
}
