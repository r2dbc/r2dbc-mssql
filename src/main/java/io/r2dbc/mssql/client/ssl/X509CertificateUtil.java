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

package io.r2dbc.mssql.client.ssl;

import reactor.util.annotation.Nullable;

import java.net.IDN;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Miscellaneous {@link X509Certificate} utility methods.
 *
 * @author Mark Paluch
 */
final class X509CertificateUtil {

    /**
     * Extract the host name from the given {@link X509Certificate}.
     *
     * @param cert the certificate.
     * @return the extracted host name.
     */
    @Nullable
    static String getHostName(X509Certificate cert) {
        return extractCommonName(cert.getSubjectX500Principal().getName("canonical"));
    }

    /**
     * Extract common name from RFC 2253 format.
     * Returns the common name if successful, {@code null} if failed to find the common name.
     *
     * @param distinguishedName the DN
     * @return the extracted host name.
     */
    @Nullable
    private static String extractCommonName(String distinguishedName) {

        int index;
        // canonical name converts entire name to lowercase
        index = distinguishedName.indexOf("cn=");
        if (index == -1) {
            return null;
        }

        distinguishedName = distinguishedName.substring(index + 3);
        // Parse until a comma or end is reached
        // Note the parser will handle gracefully (essentially will return empty string) , inside the quotes (e.g
        // cn="Foo, bar") however
        // RFC 952 says that the hostName cant have commas however the parser should not (and will not) crash if it
        // sees a , within quotes.
        for (index = 0; index < distinguishedName.length(); index++) {
            if (distinguishedName.charAt(index) == ',') {
                break;
            }
        }

        String commonName = distinguishedName.substring(0, index);
        // strip any quotes
        if (commonName.length() > 1 && ('\"' == commonName.charAt(0))) {
            if ('\"' == commonName.charAt(commonName.length() - 1)) {
                commonName = commonName.substring(1, commonName.length() - 1);
            } else {
                // Be safe the name is not ended in " return null so the common Name wont match
                commonName = null;
            }
        }

        return commonName;
    }

    /**
     * Extract SubjectAlternativeNames from the given {@link X509Certificate} and return these as {@link IDN}-unicode {@link List} of {@link String hostnames}.
     *
     * @param cert the certificate.
     * @return {@link IDN}-unicode {@link List} of {@link String hostnames}.
     * @throws CertificateParsingException if the extension cannot be decoded.
     */
    static List<String> getSubjectAlternativeNames(X509Certificate cert) throws CertificateParsingException {

        List<String> san = new ArrayList<>();
        Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();

        if (sanCollection == null) {
            return san;
        }

        // find a subjectAlternateName entry corresponding to DNS Name
        for (List<?> sanEntry : sanCollection) {

            if (sanEntry == null || sanEntry.size() < 2) {
                continue;
            }
            Object key = sanEntry.get(0);
            Object value = sanEntry.get(1);

            // Documentation(http://download.oracle.com/javase/6/docs/api/java/security/cert/X509Certificate.html):
            // "Note that the Collection returned may contain
            // more than one name of the same type."
            // So, more than one entry of dnsNameType can be present.
            // Java docs guarantee that the first entry in the list will be an integer.
            // 2 is the sequence no of a dnsName
            if (key instanceof Integer && ((Integer) key == 2) && value instanceof String) {

                // As per RFC2459, the DNSName will be in the
                // "preferred name syntax" as specified by RFC
                // 1034 and the name can be in upper or lower case.
                // And no significance is attached to case.
                // Java docs guarantee that the second entry in the list
                // will be a string for dnsName
                String dnsNameInSANCert = (String) value;

                // Use English locale to avoid Turkish i issues.
                dnsNameInSANCert = dnsNameInSANCert.toLowerCase(Locale.ENGLISH);
                san.add(IDN.toUnicode(dnsNameInSANCert));
            }
        }

        return san;
    }

}
