/*
 * Copyright 2026 the original author or authors.
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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Windows Integrated Security.
 *
 * <p>These tests require a Windows process with a valid Windows/domain identity and
 * an external SQL Server that grants that identity access. The test is skipped unless
 * {@code R2DBC_MSSQL_INTEGRATED_HOST} is configured.</p>
 */
final class WindowsIntegratedSecurityIntegrationTests {

    private static final String DATABASE = "R2DBC_MSSQL_INTEGRATED_DATABASE";

    private static final String EXPECTED_AUTH_SCHEME = "R2DBC_MSSQL_INTEGRATED_EXPECTED_AUTH_SCHEME";

    private static final String EXPECTED_USER = "R2DBC_MSSQL_INTEGRATED_EXPECTED_USER";

    private static final String HOST = "R2DBC_MSSQL_INTEGRATED_HOST";

    private static final String PORT = "R2DBC_MSSQL_INTEGRATED_PORT";

    private static final String SSL = "R2DBC_MSSQL_INTEGRATED_SSL";

    private static final String TRUST_SERVER_CERTIFICATE = "R2DBC_MSSQL_INTEGRATED_TRUST_SERVER_CERTIFICATE";

    @Test
    void shouldConnectUsingCurrentWindowsIdentity() {

        Assumptions.assumeTrue(isWindows(), "Windows Integrated Security requires Windows");

        String host = System.getenv(HOST);
        Assumptions.assumeTrue(hasText(host), HOST + " must be configured");

        MssqlConnectionConfiguration.Builder builder = MssqlConnectionConfiguration.builder()
            .host(host)
            .integratedSecurity();

        String port = System.getenv(PORT);
        if (hasText(port)) {
            builder.port(Integer.parseInt(port));
        }

        String database = System.getenv(DATABASE);
        if (hasText(database)) {
            builder.database(database);
        }

        if (Boolean.parseBoolean(System.getenv(SSL))) {
            builder.enableSsl();
        }

        if (Boolean.parseBoolean(System.getenv(TRUST_SERVER_CERTIFICATE))) {
            builder.trustServerCertificate();
        }

        String expectedUser = System.getenv(EXPECTED_USER);
        String expectedAuthScheme = System.getenv(EXPECTED_AUTH_SCHEME);

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(builder.build());

        Flux.usingWhen(connectionFactory.create(),
                connection -> Flux.from(connection.createStatement(
                        "SELECT SUSER_SNAME(), " +
                            "CAST(CONNECTIONPROPERTY('auth_scheme') AS varchar(40))").execute())
                    .flatMap(result -> result.map((row, rowMetadata) ->
                        new AuthenticationDetails(
                            row.get(0, String.class),
                            row.get(1, String.class)))),
                MssqlConnection::close)
            .as(StepVerifier::create)
            .assertNext(authentication -> {

                assertThat(authentication.user).isNotBlank();
                assertThat(authentication.authScheme).isNotBlank();

                assertThat(authentication.authScheme)
                    .as("Windows Integrated Security authentication scheme")
                    .isIn("KERBEROS", "NTLM");

                if (hasText(expectedUser)) {
                    assertThat(authentication.user)
                        .isEqualToIgnoringCase(expectedUser);
                }

                if (hasText(expectedAuthScheme)) {
                    assertThat(authentication.authScheme)
                        .isEqualToIgnoringCase(expectedAuthScheme);
                }
            })
            .verifyComplete();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "")
            .toLowerCase(Locale.ENGLISH)
            .contains("win");
    }

    private static final class AuthenticationDetails {

        private final String user;

        private final String authScheme;

        private AuthenticationDetails(String user, String authScheme) {
            this.user = user;
            this.authScheme = authScheme;
        }
    }
}
