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

package io.r2dbc.mssql.client.ssl;

/**
 * SSL Configuration for SQL Server connections.
 * <p>Microsoft SQL server supports various SSL setups:
 *
 * <ul>
 * <li>Not enabled</li>
 * <li>Supported</li>
 * <li>Enabled/required</li>
 * </ul>
 * <p>
 * Supported mode uses SSL during login to encrypt login credentials. SSL is disabled after login.
 * The client supports login-time SSL even when {@link #isSslEnabled()} is {@code false}. This mode does not validate certificates.
 * <p>Enabling {@link #isSslEnabled() SSL} enables also SSL certificate validation using {@link #getHostNameInCertificate()}.
 *
 * @author Mark Paluch
 */
public interface SslConfiguration {

    /**
     * @return {@code true} if SSL is enabled. Enabling SSL enables certificate validation. {@code false} to disable SSL.
     */
    boolean isSslEnabled();

    /**
     * @return expected hostname in the SSL certificate.
     */
    String getHostNameInCertificate();
}
