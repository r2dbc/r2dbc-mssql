/*
 * Copyright 2019 the original author or authors.
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

import reactor.core.publisher.Flux;

/**
 * Operator utility.
 *
 * @author Mark Paluch
 */
public final class Operators {

    private Operators() {
    }

    /**
     * Replay signals from {@link Flux the source} until cancellation. Drains the source for data signals if the subscriber cancels the subscription.
     * <p>
     * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without any previous
     *
     * @param source
     * @param <T>
     * @return
     */
    public static <T> Flux<T> discardOnCancel(Flux<? extends T> source) {
        return new FluxDiscardOnCancel<>(source);
    }
}
