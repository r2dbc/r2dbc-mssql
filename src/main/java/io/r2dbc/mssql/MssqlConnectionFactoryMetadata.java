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

package io.r2dbc.mssql;

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * An implementation of {@link ConnectionFactoryMetadata} for a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
enum MssqlConnectionFactoryMetadata implements ConnectionFactoryMetadata {

    INSTANCE;

    /**
     * The name of the Microsoft SQL Server database product.
     */
    public static final String NAME = "Microsoft SQL Server";

    MssqlConnectionFactoryMetadata() {
    }

    @Override
    public String getName() {
        return NAME;
    }
}
