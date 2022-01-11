/*
 * Copyright 2020-2022 the original author or authors.
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

import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link X509CertificateUtil}.
 *
 * @author Mark Paluch
 */
class X509CertificateUtilUnitTests {

    @Test
    void shouldCorrectlyExtractSan() throws GeneralSecurityException {

        X509Certificate mockCert = mock(X509Certificate.class);
        when(mockCert.getSubjectAlternativeNames()).thenReturn(Arrays.asList(Arrays.asList(1, "invalid"), Arrays.asList(2, "valid"), Collections.singletonList(3)));

        List<String> names = X509CertificateUtil.getSubjectAlternativeNames(mockCert);

        assertThat(names).containsOnly("valid");
    }
}
