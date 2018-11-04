/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.CursoredQueryMessageFlow.CursorState.Phase;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.DoneInProcToken;
import io.r2dbc.mssql.message.token.DoneProcToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SynchronousSink;

import java.util.Objects;

/**
 * Query message flow using cursors. The cursored query message flow uses {@link RpcRequest RPC} calls to open, fetch and close cursors.
 *
 * @author Mark Paluch
 * @see RpcRequest
 */
final class CursoredQueryMessageFlow {

    static final RpcRequest.OptionFlags NO_METADATA = RpcRequest.OptionFlags.empty().disableMetadata();

    // Constants for server-cursored result sets.
    // See the Engine Cursors Functional Specification for details.
    static final int FETCH_FIRST = 1;

    static final int FETCH_NEXT = 2;

    static final int FETCH_PREV = 4;

    static final int FETCH_LAST = 8;

    static final int FETCH_ABSOLUTE = 16;

    static final int FETCH_RELATIVE = 32;

    static final int FETCH_REFRESH = 128;

    static final int FETCH_INFO = 256;

    static final int FETCH_PREV_NOADJUST = 512;

    static final int SCROLLOPT_FAST_FORWARD = 16;

    static final int CCOPT_READ_ONLY = 1;

    static final int CCOPT_ALLOW_DIRECT = 8192;

    /**
     * Execute a cursored query.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param codecs    the codecs to decode {@link ReturnValue}s from RPC calls.
     * @param query     the query to execute.
     * @param fetchSize the number of rows to fetch. TODO: Try to determine fetch size from current demand and apply demand function.
     * @return the messages received in response to this exchange.
     */
    static Flux<Message> exchange(Client client, Codecs codecs, String query, int fetchSize) {

        Objects.requireNonNull(client, "Client must not be null");
        Objects.requireNonNull(query, "Query must not be null");

        EmitterProcessor<ClientMessage> emitterProcessor = EmitterProcessor.create(false);
        FluxSink<ClientMessage> requests = emitterProcessor.sink();

        CursorState state = new CursorState();

        return client.exchange(emitterProcessor.startWith(spCursorOpen(query, client.getDatabaseCollation().get(), client.getTransactionDescriptor()))) //
            .doOnSubscribe(ignore -> QueryLogger.logQuery(query))
            .doOnNext(it -> {

                if (it instanceof ReturnValue) {

                    ReturnValue returnValue = (ReturnValue) it;

                    // cursor Id
                    if (returnValue.getOrdinal() == 0) {

                        Column column = Column.from(returnValue);
                        state.cursorId = codecs.decode(returnValue.getValue(), column, Integer.class);
                    }

                    returnValue.release();
                }

                if (it instanceof RowToken) {
                    state.hasSeenRows = true;
                }

                if (it instanceof ErrorToken) {
                    state.hasSeenError = true;
                }
            })
            .handle((message, sink) -> {

                sink.next(message);

                handleStateChange(client, fetchSize, requests, state, message, sink);
            });
    }

    private static void handleStateChange(Client client, int fetchSize, FluxSink<ClientMessage> requests, CursorState state, Message message, SynchronousSink<Message> sink) {

        if (message instanceof DoneInProcToken) {

            DoneInProcToken doneToken = (DoneInProcToken) message;
            state.hasMore = doneToken.hasMore();
        }

        if (!(message instanceof DoneProcToken)) {
            return;
        }

        if (state.hasSeenError) {
            state.phase = Phase.ERROR;
            return;
        }

        if (DoneProcToken.isDone(message)) {
            onDone(client, fetchSize, requests, state, sink::complete);
        }
    }

    static void onDone(Client client, int fetchSize, FluxSink<ClientMessage> requests, CursorState state, Runnable completeSignal) {

        Phase phase = state.phase;

        if (phase == Phase.NONE || phase == Phase.FETCHING) {

            if ((state.hasMore && phase == Phase.NONE) || state.hasSeenRows) {
                if (phase == Phase.NONE) {
                    state.phase = Phase.FETCHING;
                }
                requests.next(spCursorFetch(state.cursorId, FETCH_NEXT, fetchSize, client.getTransactionDescriptor()));
            } else {
                state.phase = Phase.CLOSING;
                // TODO: spCursorClose should happen also if a subscriber cancels its subscription. 
                requests.next(spCursorClose(state.cursorId, client.getTransactionDescriptor()));
            }

            state.hasSeenRows = false;
        }

        if (phase == Phase.ERROR || phase == Phase.CLOSING) {

            completeSignal.run();
            state.phase = Phase.CLOSED;
        }
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen} to execute a SQL statement that returns a cursor.
     *
     * @param query                 the query to execute.
     * @param collation             the database collation.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen}.
     */
    static RpcRequest spCursorOpen(String query, Collation collation, TransactionDescriptor transactionDescriptor) {

        Objects.requireNonNull(query, "Query must not be null");
        Objects.requireNonNull(collation, "Collation must not be null");
        Objects.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        int resultSetScrollOpt = SCROLLOPT_FAST_FORWARD;
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorOpen) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, collation, query)
            .withParameter(RpcDirection.IN, resultSetScrollOpt)  // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0) // rowcount
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch} to fetch {@code rowCount} from the given {@literal cursor}.
     *
     * @param cursor                the cursor Id.
     * @param fetchType             the type of fetch operation (first, next, …).
     * @param rowCount              number of rows to fetch
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorFetch(int cursor, int fetchType, int rowCount, TransactionDescriptor transactionDescriptor) {

        Assert.isTrue(rowCount >= 0, "Row count must be greater or equal to zero");
        Objects.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorFetch) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withOptionFlags(NO_METADATA) //
            .withParameter(RpcDirection.IN, cursor) // cursor
            .withParameter(RpcDirection.IN, fetchType) // fetch type
            .withParameter(RpcDirection.IN, 0)  // startRow
            .withParameter(RpcDirection.IN, rowCount) // numRows
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorClose} release server resources.
     *
     * @param cursor                the cursor Id.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorClose(int cursor, TransactionDescriptor transactionDescriptor) {

        Objects.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorClose) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.IN, cursor) // cursor
            .build();
    }

    /**
     * Cursoring state.
     */
    static class CursorState {

        volatile int cursorId;

        // hasMore flag from the DoneInProc token
        volatile boolean hasMore;

        // hasMore typically reports true, but we need to check whether we've seen rows to determine whether to end cursoring.
        volatile boolean hasSeenRows;

        volatile boolean hasSeenError;

        Phase phase = Phase.NONE;

        enum Phase {
            NONE, FETCHING, CLOSING, CLOSED, ERROR
        }
    }
}
