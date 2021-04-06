/*
 * Copyright 2019-2021 the original author or authors.
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

import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import javax.net.ssl.X509TrustManager;
import java.net.IDN;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.function.Predicate;

/**
 * {@link X509TrustManager} implementation using {@link HostNamePredicate} to verify {@link X509Certificate}s.
 *
 * @author Mark Paluch
 */
public final class ExpectedHostnameX509TrustManager implements X509TrustManager {

    private static final Logger logger = Loggers.getLogger(TdsSslHandler.class);

    private final X509TrustManager defaultTrustManager;

    private final String expectedHostName;

    private final Predicate<String> matcher;

    public ExpectedHostnameX509TrustManager(X509TrustManager tm, String expectedHostName) {

        this.defaultTrustManager = tm;
        this.expectedHostName = expectedHostName;
        this.matcher = HostNamePredicate.of(expectedHostName);
    }

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

        if (logger.isDebugEnabled()) {
            logger.debug("Forwarding ClientTrusted");
        }

        this.defaultTrustManager.checkClientTrusted(chain, authType);
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

        if (logger.isDebugEnabled()) {
            logger.debug("Forwarding ServerTrusted");
        }

        this.defaultTrustManager.checkServerTrusted(chain, authType);

        if (logger.isDebugEnabled()) {
            logger.debug("ServerTrusted succeeded proceeding with server name validation");
        }

        validateServerNameInCertificate(chain[0]);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return this.defaultTrustManager.getAcceptedIssuers();
    }

    private void validateServerNameInCertificate(X509Certificate cert) throws CertificateException {

        String subjectCN = X509CertificateUtil.getHostName(cert);

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Expecting server name: [%s]", this.expectedHostName));
            logger.debug(String.format("Name in certificate: [%s]", subjectCN));
        }

        boolean isServerNameValidated;

        // the name in cert is in RFC2253 format parse it to get the actual subject name

        isServerNameValidated = validateServerName(subjectCN);

        if (!isServerNameValidated) {

            List<String> subjectAlternativeNames = X509CertificateUtil.getSubjectAlternativeNames(cert);

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Expecting server name validation failed. Checking alternative names (SAN) [%s]", subjectAlternativeNames));
            }

            for (String subjectAlternativeName : subjectAlternativeNames) {

                isServerNameValidated = validateServerName(subjectAlternativeName);

                if (isServerNameValidated) {
                    break;
                }
            }
        }

        if (!isServerNameValidated) {
            throw new CertificateException(String.format("Cannot validate certificate: %s", subjectCN));
        }
    }

    private boolean validateServerName(@Nullable String nameInCert) {

        // Failed to get the common name from DN or empty CN
        if (null == nameInCert) {
            return false;
        }

        if (nameInCert.startsWith("xn--")) {
            nameInCert = IDN.toUnicode(nameInCert);
        }

        boolean matches = this.matcher.test(nameInCert);

        if (matches) {
            logSuccessMessage(nameInCert);
        } else {
            logFailMessage(nameInCert);
        }

        return matches;
    }

    private void logFailMessage(String nameInCert) {

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("The name in certificate [%s] does not match with the server name [%s].", nameInCert, this.expectedHostName));
        }
    }

    private void logSuccessMessage(String nameInCert) {

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("The name in certificate [%s] validated against server name [%s].", nameInCert, this.expectedHostName));
        }
    }

}
