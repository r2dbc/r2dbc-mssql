/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.mssql.client.ssl;

import io.r2dbc.mssql.message.token.Login7;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
 * Accepts all {@link X509Certificate}s. Used when SSL is not enabled on the client side to allow SSL during {@link Login7} exchange.
 *
 * @author Mark Paluch
 */
enum TrustAllTrustManager implements X509TrustManager {

    INSTANCE;

    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
