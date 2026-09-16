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

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.Sspi.CredHandle;
import com.sun.jna.platform.win32.Sspi.CtxtHandle;
import com.sun.jna.platform.win32.Sspi.TimeStamp;
import com.sun.jna.platform.win32.SspiUtil.ManagedSecBufferDesc;
import com.sun.jna.platform.win32.W32Errors;
import com.sun.jna.ptr.IntByReference;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WindowsSspiAuthentication}.
 *
 * @author Daniel Pachali
 */
final class WindowsSspiAuthenticationUnitTests {

    @Test
    void shouldProduceInitialAndContinuationTokensAndReleaseResources() {

        byte[] initialToken = new byte[]{0x60, 0x01, 0x02};
        byte[] challenge = new byte[]{0x11, 0x12};
        byte[] response = new byte[]{0x21, 0x22, 0x23};

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.addResult(W32Errors.SEC_I_CONTINUE_NEEDED, initialToken);
        sspi.addResult(W32Errors.SEC_E_OK, response);

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectNextMatches(actual -> Arrays.equals(actual, initialToken))
            .verifyComplete();

        StepVerifier.create(authentication.nextToken(challenge))
            .expectNextMatches(actual -> Arrays.equals(actual, response))
            .verifyComplete();

        assertThat(sspi.acquireCount).isEqualTo(1);
        assertThat(sspi.inputs).hasSize(2);
        assertThat(sspi.inputs.get(0)).isNull();
        assertThat(sspi.inputs.get(1)).containsExactly(challenge);
        assertThat(sspi.targetNames).containsOnly("MSSQLSvc/sql.example.com:1433");

        StepVerifier.create(authentication.close()).verifyComplete();
        StepVerifier.create(authentication.close()).verifyComplete();

        assertThat(sspi.deleteCount).isEqualTo(1);
        assertThat(sspi.freeCount).isEqualTo(1);
    }

    @Test
    void shouldUseSecurityPackageMaximumTokenSize() {

        byte[] initialToken = new byte[]{0x60, 0x01};
        byte[] challenge = new byte[]{0x11};
        byte[] response = new byte[]{0x21, 0x22};

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.maxTokenSize = 32768;
        sspi.addResult(W32Errors.SEC_I_CONTINUE_NEEDED, initialToken);
        sspi.addResult(W32Errors.SEC_E_OK, response);

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectNextMatches(actual -> Arrays.equals(actual, initialToken))
            .verifyComplete();

        StepVerifier.create(authentication.nextToken(challenge))
            .expectNextMatches(actual -> Arrays.equals(actual, response))
            .verifyComplete();

        assertThat(sspi.queryMaxTokenSizeCount).isEqualTo(1);
        assertThat(sspi.outputBufferSizes).containsExactly(32768, 32768);

        StepVerifier.create(authentication.close()).verifyComplete();
    }

    @Test
    void shouldRejectInvalidSecurityPackageMaximumTokenSize() {

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.maxTokenSize = 0;

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectErrorMatches(error -> error instanceof IllegalStateException &&
                error.getMessage().contains("positive maximum token size"))
            .verify();

        assertThat(sspi.queryMaxTokenSizeCount).isEqualTo(1);
        assertThat(sspi.acquireCount).isZero();

        StepVerifier.create(authentication.close()).verifyComplete();
    }
    @Test
    void shouldCompleteWithoutFinalClientToken() {

        byte[] initialToken = new byte[]{0x60, 0x01, 0x02};
        byte[] finalServerToken = new byte[]{0x31, 0x32};

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.addResult(W32Errors.SEC_I_CONTINUE_NEEDED, initialToken);
        sspi.addResult(W32Errors.SEC_E_OK, new byte[0]);

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectNextMatches(actual -> Arrays.equals(actual, initialToken))
            .verifyComplete();

        StepVerifier.create(authentication.nextToken(finalServerToken))
            .verifyComplete();

        assertThat(sspi.inputs).hasSize(2);
        assertThat(sspi.inputs.get(1)).containsExactly(finalServerToken);

        StepVerifier.create(authentication.close()).verifyComplete();
    }

    @Test
    void shouldCompleteSspiTokenWhenRequested() {

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.addResult(0x00090314, new byte[]{0x60, 0x01});

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectNextMatches(actual -> Arrays.equals(actual, new byte[]{0x60, 0x01}))
            .verifyComplete();

        assertThat(sspi.completeCount).isEqualTo(1);

        StepVerifier.create(authentication.close()).verifyComplete();
    }

    @Test
    void shouldPropagateAcquireCredentialsFailure() {

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.acquireStatus = 0x8009030E;

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectErrorMatches(error -> error instanceof IllegalStateException &&
                error.getMessage().contains("AcquireCredentialsHandle") &&
                error.getMessage().contains("0x8009030E"))
            .verify();

        StepVerifier.create(authentication.close()).verifyComplete();

        assertThat(sspi.freeCount).isZero();
        assertThat(sspi.deleteCount).isZero();
    }

    @Test
    void shouldFreeCredentialsAfterContextInitializationFailure() {

        TestSspiSupport sspi = new TestSspiSupport();
        sspi.addResult(0x80090302, new byte[0]);

        WindowsSspiAuthentication authentication = new WindowsSspiAuthentication(
            "MSSQLSvc/sql.example.com:1433", sspi, Schedulers.immediate());

        StepVerifier.create(authentication.initialToken())
            .expectErrorMatches(error -> error instanceof IllegalStateException &&
                error.getMessage().contains("InitializeSecurityContext") &&
                error.getMessage().contains("0x80090302"))
            .verify();

        StepVerifier.create(authentication.close()).verifyComplete();

        assertThat(sspi.freeCount).isEqualTo(1);
        assertThat(sspi.deleteCount).isZero();
    }

    private static final class TestSspiSupport implements WindowsSspiAuthentication.SspiSupport {

        private final List<Result> results = new ArrayList<>();

        private final List<byte[]> inputs = new ArrayList<>();

        private final List<String> targetNames = new ArrayList<>();

        private final List<Integer> outputBufferSizes = new ArrayList<>();

        private int maxTokenSize = 48000;

        private int queryMaxTokenSizeCount;

        private int acquireStatus = W32Errors.SEC_E_OK;

        private int acquireCount;

        private int completeCount;

        private int deleteCount;

        private int freeCount;

        private int resultIndex;

        void addResult(int status, byte[] token) {
            this.results.add(new Result(status, token));
        }

        @Override
        public int queryMaxTokenSize() {
            this.queryMaxTokenSizeCount++;
            return this.maxTokenSize;
        }
        @Override
        public int acquireCredentialsHandle(CredHandle credentials, TimeStamp expiry) {

            this.acquireCount++;

            if (this.acquireStatus == W32Errors.SEC_E_OK) {
                credentials.dwLower = Pointer.createConstant(1);
                credentials.dwUpper = Pointer.createConstant(2);
            }

            return this.acquireStatus;
        }

        @Override
        public int initializeSecurityContext(CredHandle credentials, @Nullable CtxtHandle context, String targetName,
                                             @Nullable ManagedSecBufferDesc input, CtxtHandle newContext,
                                             ManagedSecBufferDesc output, IntByReference contextAttributes) {

            this.targetNames.add(targetName);
            this.inputs.add(input == null ? null : input.getBuffer(0).getBytes());
            this.outputBufferSizes.add(output.getBuffer(0).cbBuffer);

            Result result = this.results.get(this.resultIndex++);

            if (result.status == W32Errors.SEC_E_OK ||
                result.status == W32Errors.SEC_I_CONTINUE_NEEDED ||
                result.status == 0x00090313 ||
                result.status == 0x00090314) {
                newContext.dwLower = Pointer.createConstant(3);
                newContext.dwUpper = Pointer.createConstant(4);
            }

            if (result.token.length > 0) {
                output.getBuffer(0).pvBuffer.write(0, result.token, 0, result.token.length);
            }
            output.getBuffer(0).cbBuffer = result.token.length;

            return result.status;
        }

        @Override
        public int completeAuthToken(CtxtHandle context, ManagedSecBufferDesc token) {
            this.completeCount++;
            return W32Errors.SEC_E_OK;
        }

        @Override
        public int deleteSecurityContext(CtxtHandle context) {
            this.deleteCount++;
            return W32Errors.SEC_E_OK;
        }

        @Override
        public int freeCredentialsHandle(CredHandle credentials) {
            this.freeCount++;
            return W32Errors.SEC_E_OK;
        }

    }

    private static final class Result {

        private final int status;

        private final byte[] token;

        private Result(int status, byte[] token) {
            this.status = status;
            this.token = token;
        }

    }

}
