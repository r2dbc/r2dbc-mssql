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

package io.r2dbc.mssql.client;

import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for {@link ReactorNettyClient}.
 */
class ReactorNettyClientIntegrationTests extends IntegrationTestSupport {

    static final Field CONNECTION = ReflectionUtils.findField(ReactorNettyClient.class, "connection");

    static final Field CLIENT = ReflectionUtils.findField(MssqlConnection.class, "client");

    static {
        ReflectionUtils.makeAccessible(CONNECTION);
        ReflectionUtils.makeAccessible(CLIENT);
    }

    private ReactorNettyClient client;

    private Connection connection;

    @BeforeEach
    void setUp() {
        this.client = (ReactorNettyClient) ReflectionUtils.getField(CLIENT, IntegrationTestSupport.connection);
        this.connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);
    }

    @Test
    void disconnectedShouldRejectExchange() throws InterruptedException {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);
        connection.channel().close().awaitUninterruptibly();

        this.client.close()
            .thenMany(this.client.exchange(Mono.empty(), message -> true))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @Test
    void shouldCancelExchangeOnCloseFirstMessage() throws Exception {

        Sinks.Many<ClientMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        Flux<Message> query = this.client.exchange(messages.asFlux(), message -> true);
        CompletableFuture<List<Message>> future = query.collectList().toFuture();

        this.connection.channel().eventLoop().execute(() -> {

            this.connection.channel().close();

            SqlBatch batch = SqlBatch.create(0, this.client.getTransactionDescriptor(), "SELECT value FROM test");
            messages.tryEmitNext(batch);
        });

        try {
            future.get(9995, TimeUnit.SECONDS);
            fail("Expected MssqlConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(ReactorNettyClient.MssqlConnectionClosedException.class).hasMessageContaining("closed");
        }
    }

    @Test
    void shouldCancelExchangeOnCloseInFlight() throws Exception {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);

        SqlBatch batch = SqlBatch.create(0, this.client.getTransactionDescriptor(), "SELECT value FROM test");

        Sinks.Many<ClientMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        Flux<Message> query = this.client.exchange(messages.asFlux(), message -> true);
        CompletableFuture<List<Message>> future = query.doOnNext(ignore -> {
            connection.channel().close();
            messages.tryEmitNext(batch);

        }).collectList().toFuture();

        messages.tryEmitNext(batch);

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected MssqlConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(ReactorNettyClient.MssqlConnectionClosedException.class).hasMessageContaining("closed");
        }
    }

}
