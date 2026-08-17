/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.mssql.client;

import io.netty.handler.ssl.SslContext;
import io.r2dbc.mssql.client.ssl.SslConfiguration;

enum DisabledSslTunnel implements SslConfiguration {

    INSTANCE;

    @Override
    public boolean isSslEnabled() {
        return false;
    }

    @Override
    public SslContext getSslContext() {
        throw new IllegalStateException("SSL tunnel is disabled");
    }
}
