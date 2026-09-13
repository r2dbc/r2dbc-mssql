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

import com.sun.jna.Platform;
import com.sun.jna.platform.win32.Secur32;
import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.Sspi.PSecPkgInfo;
import com.sun.jna.platform.win32.Sspi.CredHandle;
import com.sun.jna.platform.win32.Sspi.CtxtHandle;
import com.sun.jna.platform.win32.Sspi.TimeStamp;
import com.sun.jna.platform.win32.SspiUtil.ManagedSecBufferDesc;
import com.sun.jna.platform.win32.W32Errors;
import com.sun.jna.ptr.IntByReference;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import java.util.Arrays;

/**
 * Windows SSPI implementation of {@link IntegratedAuthentication}.
 *
 * Uses the credentials of the current Windows process and the Negotiate security package so Windows can select
 * Kerberos or NTLM as appropriate.
 *
 * @author Daniel Pachali
 */
final class WindowsSspiAuthentication implements IntegratedAuthentication {

    private static final String SECURITY_PACKAGE = "Negotiate";

    private static final int SEC_I_COMPLETE_NEEDED = 0x00090313;

    private static final int SEC_I_COMPLETE_AND_CONTINUE = 0x00090314;

    private final String targetName;

    private final SspiSupport sspi;

    private final Scheduler scheduler;

    private int maxTokenSize;

    private final CredHandle credentials = new CredHandle();

    private final CtxtHandle context = new CtxtHandle();

    private boolean credentialsAcquired;

    private boolean contextAcquired;

    private boolean started;

    private boolean closed;

    /**
     * Create Windows integrated authentication for a service principal name.
     *
     * @param targetName the target service principal name, for example {@code MSSQLSvc/sql.example.com:1433}.
     */
    WindowsSspiAuthentication(String targetName) {
        this(targetName, createNativeSupport(), Schedulers.boundedElastic());
    }

    WindowsSspiAuthentication(String targetName, SspiSupport sspi, Scheduler scheduler) {

        this.targetName = Assert.requireNonNull(targetName, "Target name must not be null");
        Assert.isTrue(targetName.trim().length() > 0, "Target name must not be empty");

        this.sspi = Assert.requireNonNull(sspi, "SSPI support must not be null");
        this.scheduler = Assert.requireNonNull(scheduler, "Scheduler must not be null");
    }

    @Override
    public Mono<byte[]> initialToken() {
        return Mono.fromCallable(() -> createToken(null, true)).subscribeOn(this.scheduler);
    }

    @Override
    public Mono<byte[]> nextToken(byte[] serverToken) {

        Assert.requireNonNull(serverToken, "Server SSPI token must not be null");

        byte[] token = Arrays.copyOf(serverToken, serverToken.length);

        return Mono.fromCallable(() -> createToken(token, false)).subscribeOn(this.scheduler);
    }

    @Override
    public Mono<Void> close() {
        return Mono.fromRunnable(this::releaseResources).subscribeOn(this.scheduler).then();
    }

    @Nullable
    private synchronized byte[] createToken(@Nullable byte[] inputToken, boolean initial) {

        Assert.state(!this.closed, "Integrated authentication is already closed");

        if (initial) {
            Assert.state(!this.started, "Initial SSPI token has already been created");
            this.maxTokenSize = this.sspi.queryMaxTokenSize();
            Assert.state(this.maxTokenSize > 0,
                "Negotiate security package must report a positive maximum token size");
            acquireCredentials();
            this.started = true;
        } else {
            Assert.state(this.started, "Initial SSPI token has not been created");
            Assert.state(this.contextAcquired, "SSPI security context has not been created");
        }

        ManagedSecBufferDesc input = inputToken == null
            ? null
            : new ManagedSecBufferDesc(Sspi.SECBUFFER_TOKEN, inputToken);
        ManagedSecBufferDesc output = new ManagedSecBufferDesc(Sspi.SECBUFFER_TOKEN, this.maxTokenSize);

        int status = this.sspi.initializeSecurityContext(this.credentials,
            this.contextAcquired ? this.context : null, this.targetName, input, this.context, output,
            new IntByReference());

        if (!this.context.isNull()) {
            this.contextAcquired = true;
        }

        if (status == SEC_I_COMPLETE_NEEDED || status == SEC_I_COMPLETE_AND_CONTINUE) {

            int completeStatus = this.sspi.completeAuthToken(this.context, output);

            if (completeStatus != W32Errors.SEC_E_OK) {
                throw sspiFailure("CompleteAuthToken", completeStatus);
            }
        }

        if (status != W32Errors.SEC_E_OK &&
            status != W32Errors.SEC_I_CONTINUE_NEEDED &&
            status != SEC_I_COMPLETE_NEEDED &&
            status != SEC_I_COMPLETE_AND_CONTINUE) {
            throw sspiFailure("InitializeSecurityContext", status);
        }

        byte[] token = output.getBuffer(0).getBytes();

        if (initial && (token == null || token.length == 0)) {
            throw new IllegalStateException("InitializeSecurityContext did not produce an initial SSPI token");
        }

        if (!initial && status == W32Errors.SEC_E_OK && (token == null || token.length == 0)) {
            return null;
        }

        return token == null ? new byte[0] : token;
    }

    private void acquireCredentials() {

        TimeStamp expiry = new TimeStamp();

        int status = this.sspi.acquireCredentialsHandle(this.credentials, expiry);

        if (status != W32Errors.SEC_E_OK) {
            throw sspiFailure("AcquireCredentialsHandle", status);
        }

        this.credentialsAcquired = true;
    }

    private synchronized void releaseResources() {

        if (this.closed) {
            return;
        }

        this.closed = true;

        RuntimeException failure = null;

        if (this.contextAcquired && !this.context.isNull()) {

            int status = this.sspi.deleteSecurityContext(this.context);

            if (status != W32Errors.SEC_E_OK) {
                failure = sspiFailure("DeleteSecurityContext", status);
            }

            this.contextAcquired = false;
        }

        if (this.credentialsAcquired && !this.credentials.isNull()) {

            int status = this.sspi.freeCredentialsHandle(this.credentials);

            if (status != W32Errors.SEC_E_OK && failure == null) {
                failure = sspiFailure("FreeCredentialsHandle", status);
            }

            this.credentialsAcquired = false;
        }

        if (failure != null) {
            throw failure;
        }
    }

    private static SspiSupport createNativeSupport() {

        if (!Platform.isWindows()) {
            throw new IllegalStateException("Windows integrated authentication is only supported on Windows");
        }

        return new NativeSspiSupport(Secur32.INSTANCE);
    }

    private static IllegalStateException sspiFailure(String operation, int status) {
        return new IllegalStateException(String.format("%s failed with SSPI status 0x%08X", operation, status));
    }

    interface SspiSupport {

        int queryMaxTokenSize();

        int acquireCredentialsHandle(CredHandle credentials, TimeStamp expiry);

        int initializeSecurityContext(CredHandle credentials, @Nullable CtxtHandle context, String targetName,
                                      @Nullable ManagedSecBufferDesc input, CtxtHandle newContext,
                                      ManagedSecBufferDesc output, IntByReference contextAttributes);

        int completeAuthToken(CtxtHandle context, ManagedSecBufferDesc token);

        int deleteSecurityContext(CtxtHandle context);

        int freeCredentialsHandle(CredHandle credentials);

    }

    private static final class NativeSspiSupport implements SspiSupport {

        private final Secur32 secur32;

        private NativeSspiSupport(Secur32 secur32) {
            this.secur32 = secur32;
        }

        @Override
        public int queryMaxTokenSize() {

            PSecPkgInfo packageInfo = new PSecPkgInfo();
            int status = this.secur32.QuerySecurityPackageInfo(SECURITY_PACKAGE, packageInfo);

            if (status != W32Errors.SEC_E_OK) {
                throw sspiFailure("QuerySecurityPackageInfo", status);
            }

            try {

                if (packageInfo.pPkgInfo == null) {
                    throw new IllegalStateException(
                        "QuerySecurityPackageInfo did not return security package information");
                }

                return packageInfo.pPkgInfo.cbMaxToken;
            } finally {

                if (packageInfo.pPkgInfo != null) {
                    int freeStatus = this.secur32.FreeContextBuffer(packageInfo.pPkgInfo.getPointer());

                    if (freeStatus != W32Errors.SEC_E_OK) {
                        throw sspiFailure("FreeContextBuffer", freeStatus);
                    }
                }
            }
        }
        @Override
        public int acquireCredentialsHandle(CredHandle credentials, TimeStamp expiry) {
            return this.secur32.AcquireCredentialsHandle(null, SECURITY_PACKAGE, Sspi.SECPKG_CRED_OUTBOUND,
                null, null, null, null, credentials, expiry);
        }

        @Override
        public int initializeSecurityContext(CredHandle credentials, @Nullable CtxtHandle context, String targetName,
                                             @Nullable ManagedSecBufferDesc input, CtxtHandle newContext,
                                             ManagedSecBufferDesc output, IntByReference contextAttributes) {

            return this.secur32.InitializeSecurityContext(credentials, context, targetName, Sspi.ISC_REQ_CONNECTION,
                0, Sspi.SECURITY_NATIVE_DREP, input, 0, newContext, output, contextAttributes, null);
        }

        @Override
        public int completeAuthToken(CtxtHandle context, ManagedSecBufferDesc token) {
            return this.secur32.CompleteAuthToken(context, token);
        }

        @Override
        public int deleteSecurityContext(CtxtHandle context) {
            return this.secur32.DeleteSecurityContext(context);
        }

        @Override
        public int freeCredentialsHandle(CredHandle credentials) {
            return this.secur32.FreeCredentialsHandle(credentials);
        }

    }

}
