/*
 * Copyright 2018 the original author or authors.
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

import io.r2dbc.mssql.message.tds.Redirect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.org.bouncycastle.asn1.x500.X500Name;
import org.testcontainers.shaded.org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.testcontainers.shaded.org.bouncycastle.cert.X509v3CertificateBuilder;
import org.testcontainers.shaded.org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.testcontainers.shaded.org.bouncycastle.operator.ContentSigner;
import org.testcontainers.shaded.org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import reactor.netty.resources.ConnectionProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link MssqlConnectionConfiguration}.
 *
 * @author Mark Paluch
 * @author Paul Johe
 */
final class MssqlConnectionConfigurationUnitTests {

    @Test
    void defaults() {

        MssqlConnectionConfiguration configuration = builder().build();

        assertThat(configuration.getHost()).isEqualTo("test-host");
        assertThat(configuration.getPort()).isEqualTo(MssqlConnectionConfiguration.DEFAULT_PORT);
        assertThat(configuration.getServerName()).isEqualTo("test-host");
        assertThat(configuration.getHostNameInCertificate()).isEqualTo("test-host");
        assertThat(configuration.getDatabase()).isEmpty();
        assertThat(configuration.getConnectTimeout()).isEqualTo(MssqlConnectionConfiguration.DEFAULT_CONNECT_TIMEOUT);
        assertThat(configuration.getLockWaitTimeout()).isNull();
        assertThat(configuration.isSendStringParametersAsUnicode()).isTrue();
        assertThat(configuration.useSsl()).isFalse();
        assertThat(configuration.isTcpKeepAlive()).isFalse();
        assertThat(configuration.isTcpNoDelay()).isTrue();
    }

    @Test
    void configuredValues() {

        UUID connectionId = UUID.randomUUID();
        Predicate<String> cursorPreference = sql -> true;
        ConnectionProvider connectionProvider = ConnectionProvider.create("test");

        MssqlConnectionConfiguration configuration = builder()
            .applicationName("r2dbc")
            .connectionId(connectionId)
            .connectionProvider(connectionProvider)
            .database("test-database")
            .port(100)
            .preferCursoredExecution(cursorPreference)
            .sendStringParametersAsUnicode(false)
            .build();

        assertThat(configuration.getApplicationName()).isEqualTo("r2dbc");
        assertThat(configuration.getConnectionId()).isEqualTo(connectionId);
        assertThat(configuration.getDatabase()).contains("test-database");
        assertThat(configuration.getHost()).isEqualTo("test-host");
        assertThat(configuration.getUsername()).isEqualTo("test-username");
        assertThat(configuration.getPassword()).isEqualTo("test-password");
        assertThat(configuration.getPort()).isEqualTo(100);
        assertThat(configuration.getPreferCursoredExecution()).isSameAs(cursorPreference);
        assertThat(configuration.isSendStringParametersAsUnicode()).isFalse();
        assertThat(configuration.toClientConfiguration().getConnectionProvider()).isSameAs(connectionProvider);
    }

    @Test
    void logicalServerName() {

        MssqlConnectionConfiguration configuration = builder().host("localhost").serverName("sql.example.com").build();

        assertThat(configuration.getHost()).isEqualTo("localhost");
        assertThat(configuration.getServerName()).isEqualTo("sql.example.com");
        assertThat(configuration.getHostNameInCertificate()).isEqualTo("sql.example.com");
        assertThat(configuration.toClientConfiguration().getServerName()).isEqualTo("sql.example.com");
        assertThat(configuration.getLoginConfiguration().getServerName()).isEqualTo("sql.example.com");

        MssqlConnectionConfiguration explicitCertificateName = builder().serverName("sql.example.com").hostNameInCertificate("*.example.com").build();

        assertThat(explicitCertificateName.getHostNameInCertificate()).isEqualTo("*.example.com");
    }

    @Test
    void rejectsNullArguments() {

        MssqlConnectionConfiguration.Builder builder = MssqlConnectionConfiguration.builder();

        assertThatIllegalArgumentException().isThrownBy(() -> builder.applicationName(null)).withMessage("applicationName must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> builder.connectionId(null)).withMessage("connectionId must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> builder.host(null)).withMessage("host must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> builder.password(null)).withMessage("password must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> builder.username(null)).withMessage("username must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> builder.sslContextBuilderCustomizer(null)).withMessage("sslContextBuilderCustomizer must not be null");
    }

    @Test
    void requiresHostUsernameAndPassword() {

        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().username("test-username").password("test-password").build())
            .withMessage("host must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().host("test-host").password("test-password").build())
            .withMessage("username must not be null");
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().host("test-host").username("test-username").build())
            .withMessage("password must not be null");
    }

    @Test
    void redirectTargetsAlternateServer() {

        MssqlConnectionConfiguration target = builder().build().withRedirect(Redirect.create("target", 1234));

        assertThat(target.getHost()).isEqualTo("target");
        assertThat(target.getPort()).isEqualTo(1234);
        assertThat(target.getServerName()).isEqualTo("target");
        assertThat(target.getHostNameInCertificate()).isEqualTo("test-host");
        assertThat(target.getUsername()).isEqualTo("test-username");
    }

    @Test
    void redirectRetainsLogicalServerName() {

        MssqlConnectionConfiguration configuration = builder().host("localhost").port(15433).serverName("test-host.database.windows.net").build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("worker.database.windows.net", 1234));

        assertThat(target.getHost()).isEqualTo("worker.database.windows.net");
        assertThat(target.getPort()).isEqualTo(1234);
        assertThat(target.getServerName()).isEqualTo("test-host.database.windows.net");
        assertThat(target.getHostNameInCertificate()).isEqualTo("test-host.database.windows.net");
        assertThat(target.toClientConfiguration().getServerName()).isEqualTo("test-host.database.windows.net");
        assertThat(target.getLoginConfiguration().getServerName()).isEqualTo("test-host.database.windows.net");
    }

    @Test
    void redirectRetainsHostNameInCertificateAcrossDomains() {

        MssqlConnectionConfiguration configuration = builder().host("test-host.windows.net").build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("target.other.domain", 1234));

        assertThat(target.getHostNameInCertificate()).isEqualTo("test-host.windows.net");
    }

    @Test
    void redirectNarrowsWildcardHostNameInCertificateWithinDomain() {

        MssqlConnectionConfiguration configuration = builder().host("test-host.windows.net").hostNameInCertificate("*.windows.net").build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("worker.target.windows.net", 1234));

        assertThat(target.getHostNameInCertificate()).isEqualTo("*.target.windows.net");
    }

    @Test
    void loadsCustomTrustStore(@TempDir File tempDir) throws Exception {

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(1024, new SecureRandom());
        KeyPair keypair = keyGen.generateKeyPair();

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setEntry("dummy", new KeyStore.PrivateKeyEntry(keypair.getPrivate(), new Certificate[]{selfSign(keypair, "CN=dummy")}),
            new KeyStore.PasswordProtection("key-password".toCharArray()));

        File file = new File(tempDir, getClass().getName() + ".jks");
        try (FileOutputStream stream = new FileOutputStream(file)) {
            keyStore.store(stream, "my-password".toCharArray());
        }

        MssqlConnectionConfiguration.DefaultClientConfiguration clientConfiguration = (MssqlConnectionConfiguration.DefaultClientConfiguration) builder()
            .trustStore(file)
            .trustStorePassword("my-password".toCharArray())
            .build()
            .toClientConfiguration();

        KeyStore loaded = clientConfiguration.loadCustomTrustStore();

        assertThat(loaded.getEntry("dummy", new KeyStore.PasswordProtection("key-password".toCharArray()))).isInstanceOf(KeyStore.PrivateKeyEntry.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select", "SELECT", "sElEcT"})
    void defaultCursorPreferenceAcceptsSelect(String query) {
        assertThat(MssqlConnectionConfiguration.DefaultCursorPreference.INSTANCE).accepts(query);
    }

    @ParameterizedTest
    @ValueSource(strings = {" select", "sp_cursor", "INSERT"})
    void defaultCursorPreferenceRejectsOtherQueries(String query) {
        assertThat(MssqlConnectionConfiguration.DefaultCursorPreference.INSTANCE).rejects(query);
    }

    private static MssqlConnectionConfiguration.Builder builder() {
        return MssqlConnectionConfiguration.builder().host("test-host").username("test-username").password("test-password");
    }

    private static Certificate selfSign(KeyPair keyPair, String subjectDN) throws Exception {

        Date startDate = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.add(Calendar.YEAR, 1);

        X500Name dnName = new X500Name(subjectDN);
        SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(dnName, BigInteger.valueOf(1), startDate, calendar.getTime(), dnName, subjectPublicKeyInfo);
        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());

        return new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));
    }

}
