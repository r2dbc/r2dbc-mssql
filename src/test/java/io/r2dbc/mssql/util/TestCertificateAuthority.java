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

package io.r2dbc.mssql.util;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Certificate;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * A certificate authority for testing TLS.
 */
public class TestCertificateAuthority {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final long VALID_TIME = (long) 365 * 24 * 60 * 60 * 1000;
    private final KeyPair keyPair = randomKeyPair();
    private final String issuerDn;
    private final KeyStore trustStore;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public TestCertificateAuthority(final String issuerDn) {
        this.issuerDn = issuerDn;
        this.trustStore = createTrustStore(sign(issuerDn, issuerDn, keyPair.getPrivate(), keyPair.getPublic(), true));
    }

    private static KeyStore createTrustStore(final X509Certificate cert) {
        try {
            final KeyStore keystore = KeyStore.getInstance("jks");
            keystore.load(null, null);
            keystore.setCertificateEntry("ca", cert);
            return keystore;
        } catch (final KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException("Failed to create trust store for test CA");
        }
    }

    public ServerKeyPair newServerKeyPair(final String dn) {
        final KeyPair newKeyPair = randomKeyPair();
        final X509Certificate cert = sign(dn, issuerDn, keyPair.getPrivate(), newKeyPair.getPublic(), false);
        return new ServerKeyPair(newKeyPair, cert);
    }

    private static byte[] toPem(final PrivateKey key) {
        return toPem(key.getEncoded(), "PRIVATE KEY");
    }

    private static byte[] toPem(final X509Certificate cert) {
        try {
            return toPem(cert.getEncoded(), "CERTIFICATE");
        } catch (final CertificateEncodingException e) {
            throw new RuntimeException("Failed to encode certificate", e);
        }
    }

    private static byte[] toPem(final byte[] derCert, final String kind) {
        final String cert_begin = "-----BEGIN " + kind + "-----";
        final String end_cert = "-----END " + kind + "-----";
        final String data = Base64.getMimeEncoder(64, "\n".getBytes(US_ASCII)).encodeToString(derCert);
        return (cert_begin + "\n" + data + "\n" + end_cert + "\n").getBytes(US_ASCII);
    }

    private static X509Certificate sign(
            final String ownerDn,
            final String issuerDn,
            final PrivateKey signingKey,
            final PublicKey publicKey,
            final boolean isTrustAnchor
    ) {
        final Date notBefore = new Date(currentTimeMillis());
        final Date notAfter = new Date(currentTimeMillis() + VALID_TIME);
        try {
            final AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder()
                    .find("SHA256withRSA");
            final AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder()
                    .find(sigAlgId);

            final X509v3CertificateBuilder certGenerator = new X509v3CertificateBuilder(
                    new X500Name(issuerDn),
                    BigInteger.valueOf(SECURE_RANDOM.nextInt()),
                    notBefore,
                    notAfter,
                    new X500Name(ownerDn),
                    SubjectPublicKeyInfo.getInstance(publicKey.getEncoded())
            );
            if (isTrustAnchor) {
                certGenerator.addExtension(new ASN1ObjectIdentifier("2.5.29.19"), false, new BasicConstraints(true));
            }
            final ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
                    .build(PrivateKeyFactory.createKey(signingKey.getEncoded()));

            final X509CertificateHolder holder = certGenerator.build(sigGen);
            final Certificate eeX509CertificateStructure = holder.toASN1Structure();
            final CertificateFactory cf = CertificateFactory.getInstance("X.509", "BC");

            try (final InputStream stream = new ByteArrayInputStream(eeX509CertificateStructure.getEncoded())) {
                return (X509Certificate) cf.generateCertificate(stream);
            }
        } catch (final CertificateException | IOException | OperatorCreationException | NoSuchProviderException e) {
            throw new RuntimeException("Failed to sign certificate", e);
        }
    }

    private static KeyPair randomKeyPair() {
        try {
            final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            return kpg.generateKeyPair();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not create keypair for test CA", e);
        }
    }

    public KeyStore getTrustStore() {
        return trustStore;
    }

    static class ServerKeyPair {
        private final KeyPair keyPair;
        private final X509Certificate cert;

        ServerKeyPair(final KeyPair keyPair, final X509Certificate cert) {
            this.keyPair = keyPair;
            this.cert = cert;
        }

        public byte[] getPrivateKey() {
            return toPem(keyPair.getPrivate());
        }

        public byte[] getCertificate() {
            return toPem(cert);
        }
    }
}
