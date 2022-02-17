/*
 * Copyright 2018-2022 the original author or authors.
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
import org.testcontainers.shaded.org.bouncycastle.asn1.x500.X500Name;
import org.testcontainers.shaded.org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.testcontainers.shaded.org.bouncycastle.cert.X509CertificateHolder;
import org.testcontainers.shaded.org.bouncycastle.cert.X509v3CertificateBuilder;
import org.testcontainers.shaded.org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.testcontainers.shaded.org.bouncycastle.operator.ContentSigner;
import org.testcontainers.shaded.org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

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
 * Unit tests for {@link  MssqlConnectionConfiguration}.
 *
 * @author Mark Paluch
 */
final class MssqlConnectionConfigurationUnitTests {

    @Test
    void builderNoApplicationName() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().applicationName(null))
            .withMessage("applicationName must not be null");
    }

    @Test
    void builderNoConnectionId() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().connectionId(null))
            .withMessage("connectionId must not be null");
    }

    @Test
    void builderNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().host(null))
            .withMessage("host must not be null");
    }

    @Test
    void builderNoPassword() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().password(null))
            .withMessage("password must not be null");
    }

    @Test
    void builderNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder().username(null))
            .withMessage("username must not be null");
    }

    @Test
    void configuration() {
        UUID connectionId = UUID.randomUUID();
        Predicate<String> TRUE = s -> true;
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .connectionId(connectionId)
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .preferCursoredExecution(TRUE)
            .port(100)
            .username("test-username")
            .sendStringParametersAsUnicode(false)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionId", connectionId)
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("preferCursoredExecution", TRUE)
            .hasFieldOrPropertyWithValue("port", 100)
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrPropertyWithValue("sendStringParametersAsUnicode", false);
    }

    @Test
    void configurationDefaults() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .applicationName("r2dbc")
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("applicationName", "r2dbc")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 1433)
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrPropertyWithValue("sendStringParametersAsUnicode", true);
    }

    @Test
    void constructorNoNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .password("test-password")
            .username("test-username")
            .build())
            .withMessage("host must not be null");
    }

    @Test
    void constructorNoPassword() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .host("test-host")
            .username("test-username")
            .build())
            .withMessage("password must not be null");
    }

    @Test
    void constructorNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .build())
            .withMessage("username must not be null");
    }

    @Test
    void constructorNoSslCustomizer() {
        assertThatIllegalArgumentException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .sslContextBuilderCustomizer(null)
            .build())
            .withMessage("sslContextBuilderCustomizer must not be null");
    }

    @Test
    void redirect() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .applicationName("r2dbc")
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("target", 1234));

        assertThat(target)
            .hasFieldOrPropertyWithValue("applicationName", "r2dbc")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "target")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 1234)
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrPropertyWithValue("sendStringParametersAsUnicode", true)
            .hasFieldOrPropertyWithValue("hostNameInCertificate", "test-host");
    }

    @Test
    void redirectOtherDomain() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .applicationName("r2dbc")
            .database("test-database")
            .host("test-host.windows.net")
            .password("test-password")
            .username("test-username")
            .build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("target.other.domain", 1234));

        assertThat(target)
            .hasFieldOrPropertyWithValue("applicationName", "r2dbc")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "target.other.domain")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 1234)
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrPropertyWithValue("sendStringParametersAsUnicode", true)
            .hasFieldOrPropertyWithValue("hostNameInCertificate", "test-host.windows.net");
    }

    @Test
    void redirectInDomain() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .applicationName("r2dbc")
            .database("test-database")
            .host("test-host.windows.net")
            .password("test-password")
            .username("test-username")
            .hostNameInCertificate("*.windows.net")
            .build();

        MssqlConnectionConfiguration target = configuration.withRedirect(Redirect.create("worker.target.windows.net", 1234));

        assertThat(target)
            .hasFieldOrPropertyWithValue("applicationName", "r2dbc")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "worker.target.windows.net")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 1234)
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrPropertyWithValue("sendStringParametersAsUnicode", true)
            .hasFieldOrPropertyWithValue("hostNameInCertificate", "*.target.windows.net");
    }

    @Test
    void configureKeyStore(@TempDir File tempDir) throws Exception {

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(1024, new SecureRandom());
        KeyPair keypair = keyGen.generateKeyPair();

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);

        Certificate selfSignedCertificate = selfSign(keypair, "CN=dummy");

        KeyStore.Entry entry = new KeyStore.PrivateKeyEntry(keypair.getPrivate(),
            new Certificate[]{selfSignedCertificate});

        keyStore.setEntry("dummy", entry, new KeyStore.PasswordProtection("key-password".toCharArray()));

        File file = new File(tempDir, getClass().getName() + ".jks");
        try (FileOutputStream stream = new FileOutputStream(file)) {
            keyStore.store(stream, "my-password".toCharArray());
        }

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .database("test-database")
            .host("test-host.windows.net")
            .password("test-password")
            .username("test-username")
            .trustStore(file)
            .trustStorePassword("my-password".toCharArray())
            .build();

        MssqlConnectionConfiguration.DefaultClientConfiguration clientConfiguration = (MssqlConnectionConfiguration.DefaultClientConfiguration) configuration.toClientConfiguration();

        KeyStore loaded = clientConfiguration.loadCustomTrustStore();

        KeyStore.Entry loadedEntry = loaded.getEntry("dummy", new KeyStore.PasswordProtection("key-password".toCharArray()));
        assertThat(loadedEntry).isInstanceOf(KeyStore.PrivateKeyEntry.class);
    }

    private static Certificate selfSign(KeyPair keyPair, String subjectDN)
        throws Exception {

        Date startDate = new Date();
        X500Name dnName = new X500Name(subjectDN);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.add(Calendar.YEAR, 1);
        Date endDate = calendar.getTime();


        SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair
            .getPublic().getEncoded());

        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(dnName,
            BigInteger.valueOf(1), startDate, endDate, dnName, subjectPublicKeyInfo);

        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());

        X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);

        return new JcaX509CertificateConverter()
            .getCertificate(certificateHolder);
    }
}
