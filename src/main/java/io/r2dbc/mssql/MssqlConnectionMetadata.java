/*
 * Copyright 2019-2022 the original author or authors.
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

import io.r2dbc.spi.ConnectionMetadata;

/**
 * Connection metadata for a connection connected to Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
public final class MssqlConnectionMetadata implements ConnectionMetadata {

    private final String databaseVersion;

    private final String databaseProductName;

    MssqlConnectionMetadata(String databaseProductName, String databaseVersion) {
        this.databaseVersion = databaseVersion;
        this.databaseProductName = databaseProductName;
    }

    /**
     * Construct {@link MssqlConnectionMetadata} from a metadata query.
     *
     * @param edition         SQL Server edition.
     * @param version         SQL Server version number.
     * @param spVersionOutput output of {@code @@VERSION()} function.
     * @return the {@link MssqlConnectionMetadata}.
     */
    public static MssqlConnectionMetadata from(String edition, String version, String spVersionOutput) {

        String databaseProductName = spVersionOutput;

        int separator = spVersionOutput.indexOf(" - ");

        if (separator > -1) {
            databaseProductName = spVersionOutput.substring(0, separator) + " - " + edition;
        }

        return new MssqlConnectionMetadata(databaseProductName, version);
    }

    /**
     * Retrieves the name of this database product.
     *
     * @return database product name
     */
    public String getDatabaseProductName() {
        return this.databaseProductName;
    }

    /**
     * Retrieves the version of this database product.
     *
     * @return database product name
     */
    public String getDatabaseVersion() {
        return this.databaseVersion;
    }

}
