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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiConsumer;

/**
 * Simple (direct) query message flow using {@link SqlBatch}.
 * <p>
 * Commands require deferred creation because {@link Client} can be used concurrently and we must fetch the latest state (e.g. {@link TransactionDescriptor}) to issue a command with the appropriate
 * state.
 *
 * @author Mark Paluch
 */
final class QueryMessageFlow {

    /**
     * Execute a simple query using {@link SqlBatch}. Query execution terminates with a {@link DoneToken}.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param query  the query to execute.
     * @return the messages received in response to this exchange.
     */
    static Flux<Message> exchange(Client client, String query) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        return client.exchange(Mono.fromSupplier(() -> SqlBatch.create(1, client.getTransactionDescriptor(), query)), DoneToken::isDone)
            .doOnSubscribe(ignore -> QueryLogger.logQuery(client.getContext(), query))
            .handle(DoneHandler.INSTANCE);
    }

    enum DoneHandler implements BiConsumer<Message, SynchronousSink<Message>> {

        INSTANCE;

        @Override
        public void accept(Message message, SynchronousSink<Message> sink) {
            sink.next(message);

            if (DoneToken.isDone(message)) {
                sink.complete();
            }
        }
    }

}
