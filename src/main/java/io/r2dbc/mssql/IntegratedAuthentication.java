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

import reactor.core.publisher.Mono;

/**
 * Produces SSPI/SPNEGO tokens for integrated authentication.
 *
 * Implementations are responsible for maintaining authentication context between calls.
 *
 * @author Daniel Pachali
 */
interface IntegratedAuthentication {

    /**
     * Produce the initial SSPI/SPNEGO token embedded in LOGIN7.
     *
     * @return a {@link Mono} emitting exactly one authentication token.
     */
    Mono<byte[]> initialToken();

    /**
     * Process a server SSPI challenge and produce the next client response token.
     *
     * @param serverToken the server SSPI challenge.
     * @return a {@link Mono} emitting an authentication response token, or completing empty when
     * the authentication context is complete and no final client token needs to be sent to the
     * server.
     */
    Mono<byte[]> nextToken(byte[] serverToken);

    /**
     * Release authentication resources.
     *
     * @return completion signal.
     */
    Mono<Void> close();

}
