/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.mssql.RpcQueryMessageFlow.CursorState.Phase;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.AbstractInfoToken;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneInProcToken;
import io.r2dbc.mssql.message.token.DoneProcToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SynchronousSink;

import javax.annotation.processing.Completion;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.r2dbc.mssql.util.PredicateUtils.or;

/**
 * Query message flow using cursors. The cursored query message flow uses {@link RpcRequest RPC} calls to open, fetch and close cursors.
 *
 * @author Mark Paluch
 * @see RpcRequest
 */
final class RpcQueryMessageFlow {

    private static final Logger logger = LoggerFactory.getLogger(RpcQueryMessageFlow.class);

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

    final static int SCROLLOPT_PARAMETERIZED_STMT = 4096;

    static final int CCOPT_READ_ONLY = 1;

    static final int CCOPT_ALLOW_DIRECT = 8192;

    /**
     * Execute a direct query with parameters.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param query     the query to execute.
     * @param fetchSize the number of rows to fetch. TODO: Try to determine fetch size from current demand and apply demand function.
     * @return the messages received in response to this exchange.
     */
    static Flux<Message> exchange(Client client, String query, Binding binding) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        EmitterProcessor<ClientMessage> outbound = EmitterProcessor.create(false);
        FluxSink<ClientMessage> requests = outbound.sink();

        EmitterProcessor<Message> inbound = EmitterProcessor.create(false);
        Flux<Message> firstMessages = inbound.cache(10);

        CursorState state = new CursorState();
        state.directMode = true;

        Flux<Message> exchange = client.exchange(outbound.startWith(spExecuteSql(query, binding, client.getRequiredCollation(), client.getTransactionDescriptor())));

        OnCursorComplete cursorComplete = new OnCursorComplete(inbound);

        Flux<Message> messages = firstMessages //
            .doOnSubscribe(ignore -> QueryLogger.logQuery(query))
            .doOnNext(it -> {

                if (it instanceof ReturnValue) {

                    ReturnValue returnValue = (ReturnValue) it;
                    returnValue.release();
                }

                state.update(it);
            })
            .handle(MssqlException::handleErrorResponse)
            .<Message>handle((message, sink) -> {
                handleMessage(client, 0, requests, state, message, sink, cursorComplete);
            })
            .filter(filterForWindow())
            .doOnCancel(cursorComplete);

        return messages.doOnSubscribe(ignore -> exchange.doOnSubscribe(cursorComplete::set).subscribe(inbound));
    }

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

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        EmitterProcessor<ClientMessage> outbound = EmitterProcessor.create(false);
        FluxSink<ClientMessage> requests = outbound.sink();

        EmitterProcessor<Message> inbound = EmitterProcessor.create(false);
        Flux<Message> firstMessages = inbound.cache(10);

        CursorState state = new CursorState();

        Flux<Message> exchange = client.exchange(outbound.startWith(spCursorOpen(query, client.getRequiredCollation(), client.getTransactionDescriptor())));

        OnCursorComplete cursorComplete = new OnCursorComplete(inbound);

        Flux<Message> messages = firstMessages //
            .doOnSubscribe(ignore -> QueryLogger.logQuery(query))
            .doOnNext(it -> {

                if (it instanceof ReturnValue) {

                    ReturnValue returnValue = (ReturnValue) it;

                    // cursor Id
                    if (returnValue.getOrdinal() == 0) {
                        state.cursorId = parseCursorId(codecs, state, returnValue);
                    }

                    returnValue.release();
                }

                state.update(it);
            })
            .handle(MssqlException::handleErrorResponse)
            .<Message>handle((message, sink) -> {
                handleMessage(client, fetchSize, requests, state, message, sink, cursorComplete);
            })
            .filter(filterForWindow())
            .doOnCancel(cursorComplete);

        return messages.doOnSubscribe(ignore -> exchange.doOnSubscribe(cursorComplete::set).subscribe(inbound));
    }

    /**
     * Execute a cursored query with RPC parameters.
     *
     * @param statementCache the {@link PreparedStatementCache} to keep track of prepared statement handles.
     * @param client         the {@link Client} to exchange messages with.
     * @param codecs         the codecs to decode {@link ReturnValue}s from RPC calls.
     * @param query          the query to execute.
     * @param binding        parameter bindings.
     * @param fetchSize      the number of rows to fetch. TODO: Try to determine fetch size from current demand and apply demand function.
     * @return the messages received in response to this exchange.
     * @throws IllegalArgumentException when {@link Client} or {@code query} is {@code null}.
     */
    static Flux<Message> exchange(PreparedStatementCache statementCache, Client client, Codecs codecs, String query, Binding binding, int fetchSize) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        EmitterProcessor<ClientMessage> outbound = EmitterProcessor.create(false);
        FluxSink<ClientMessage> requests = outbound.sink();

        EmitterProcessor<Message> inbound = EmitterProcessor.create(false);
        Flux<Message> firstMessages = inbound.cache(10);

        CursorState state = new CursorState();

        int handle = statementCache.getHandle(query, binding);
        boolean needsPrepare;
        RpcRequest rpcRequest;

        if (handle == PreparedStatementCache.UNPREPARED) {
            rpcRequest = spCursorPrepExec(PreparedStatementCache.UNPREPARED, query, binding, client.getRequiredCollation(), client.getTransactionDescriptor());
            needsPrepare = true;
        } else {
            rpcRequest = spCursorExec(handle, binding, client.getTransactionDescriptor());
            needsPrepare = false;
        }

        Flux<Message> exchange = client.exchange(outbound.startWith(rpcRequest));

        OnCursorComplete cursorComplete = new OnCursorComplete(inbound);

        Flux<Message> messages = firstMessages //
            .doOnSubscribe(ignore -> QueryLogger.logQuery(query))
            .doOnNext(it -> {

                if (it instanceof ReturnValue) {

                    ReturnValue returnValue = (ReturnValue) it;

                    if (needsPrepare) {

                        // prepared statement handle
                        if (returnValue.getOrdinal() == 0) {

                            int preparedStatementHandle = codecs.decode(returnValue.getValue(), returnValue.asDecodable(), Integer.class);
                            logger.debug("Prepared statement with handle: {}", preparedStatementHandle);
                            statementCache.putHandle(preparedStatementHandle, query, binding);
                        }
                    }

                    // cursor Id
                    if (returnValue.getOrdinal() == 1) {
                        state.cursorId = parseCursorId(codecs, state, returnValue);
                    }
                }

                state.update(it);
            })
            .handle(MssqlException::handleErrorResponse)
            .<Message>handle((message, sink) -> {
                handleMessage(client, fetchSize, requests, state, message, sink, cursorComplete);
            })
            .filter(filterForWindow())
            .doOnCancel(cursorComplete);

        return messages.doOnSubscribe(ignore -> exchange.doOnSubscribe(cursorComplete::set).subscribe(inbound));
    }

    private static int parseCursorId(Codecs codecs, CursorState state, ReturnValue returnValue) {

        Integer cursorId = codecs.decode(returnValue.getValue(), returnValue.asDecodable(), Integer.class);
        logger.debug("CursorId: {}", cursorId);
        return cursorId;
    }

    private static Predicate<Message> filterForWindow() {

        return or(RowToken.class::isInstance,
            ColumnMetadataToken.class::isInstance,
            DoneInProcToken.class::isInstance,
            IntermediateCount.class::isInstance,
            AbstractInfoToken.class::isInstance,
            Completion.class::isInstance);
    }

    private static void handleMessage(Client client, int fetchSize, FluxSink<ClientMessage> requests, CursorState state, Message message, SynchronousSink<Message> sink,
                                      Runnable onCursorComplete) {

        if (message instanceof ColumnMetadataToken && ((ColumnMetadataToken) message).getColumns().isEmpty()) {
            return;
        }

        if (message instanceof AbstractInfoToken) {

            // direct mode
            if (((AbstractInfoToken) message).getNumber() == 16954) {
                state.directMode = true;
            }
        }

        if (message instanceof DoneInProcToken) {

            DoneInProcToken doneToken = (DoneInProcToken) message;
            state.hasMore = doneToken.hasMore();

            if (!state.directMode) {

                if (state.phase == Phase.FETCHING && doneToken.hasCount()) {
                    sink.next(new IntermediateCount(doneToken));
                }
                return;
            }

            sink.next(doneToken);
            return;
        }


        if (!(message instanceof DoneProcToken)) {
            sink.next(message);
            return;
        }

        if (state.hasSeenError) {
            state.phase = Phase.ERROR;
        }

        if (DoneProcToken.isDone(message)) {
            onDone(client, fetchSize, requests, state, onCursorComplete);
        }
    }

    static void onDone(Client client, int fetchSize, FluxSink<ClientMessage> requests, CursorState state, Runnable completion) {

        Phase phase = state.phase;

        if (phase == Phase.NONE || phase == Phase.FETCHING) {

            if (state.cursorId == 0) {

                completion.run();
                state.phase = Phase.CLOSED;
                return;
            }

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
            return;
        }

        if (phase == Phase.ERROR || phase == Phase.CLOSING) {

            completion.run();
            state.phase = Phase.CLOSED;
        }
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_ExecuteSql} to execute a SQL statement that returns directly results.
     *
     * @param query                 the query to execute.
     * @param binding               bound parameters
     * @param collation             the database collation.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen}.
     * @throws IllegalArgumentException when {@code query}, {@link Collation}, or {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spExecuteSql(String query, Binding binding, Collation collation, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(query, "Query must not be null");
        Assert.requireNonNull(collation, "Collation must not be null");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_ExecuteSql) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.IN, collation, query) //
            .withParameter(RpcDirection.IN, collation, binding.getFormalParameters()); // formal parameter defn

        binding.forEach((name, encoded) -> {
            builder.withNamedParameter(RpcDirection.IN, name, encoded);
        });

        return builder.build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen} to execute a SQL statement that returns a cursor.
     *
     * @param query                 the query to execute.
     * @param collation             the database collation.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen}.
     * @throws IllegalArgumentException when {@code query}, {@link Collation}, or {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spCursorOpen(String query, Collation collation, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(query, "Query must not be null");
        Assert.requireNonNull(collation, "Collation must not be null");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

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
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     * @throws IllegalArgumentException when {@code rowCount} is less than zero.
     */
    static RpcRequest spCursorFetch(int cursor, int fetchType, int rowCount, TransactionDescriptor transactionDescriptor) {

        Assert.isTrue(rowCount >= 0, "Row count must be greater or equal to zero");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

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
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spCursorClose(int cursor, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorClose) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.IN, cursor) // cursor
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorPrepare} to prepare and execute a {@code query}.
     *
     * @param preparedStatementHandle handle to a previously prepared statement. This call un-prepares a previously prepared statement.
     * @param query                   the query to execute.
     * @param binding                 bound parameters
     * @param collation               the database collation.
     * @param transactionDescriptor   transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorPrepExec(int preparedStatementHandle, String query, Binding binding, Collation collation, TransactionDescriptor transactionDescriptor) {

        int resultSetScrollOpt = SCROLLOPT_FAST_FORWARD | (binding.isEmpty() ? 0 : SCROLLOPT_PARAMETERIZED_STMT);
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorPrepExec) //
            .withTransactionDescriptor(transactionDescriptor) //

            // <prepared handle>
            // IN (reprepare): Old handle to unprepare before repreparing
            // OUT: The newly prepared handle
            .withParameter(RpcDirection.OUT, preparedStatementHandle)
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, collation, binding.getFormalParameters()) // formal parameter defn
            .withParameter(RpcDirection.IN, collation, query) // statement
            .withParameter(RpcDirection.IN, resultSetScrollOpt) // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0);// rowcount

        binding.forEach((name, encoded) -> {
            builder.withNamedParameter(RpcDirection.IN, name, encoded);
        });

        return builder.build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorExecute} to and execute prepared statement.
     *
     * @param preparedStatementHandle handle to a previously prepared statement.
     * @param binding                 bound parameters
     * @param transactionDescriptor   transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorExec(int preparedStatementHandle, Binding binding, TransactionDescriptor transactionDescriptor) {

        Assert.isTrue(preparedStatementHandle != PreparedStatementCache.UNPREPARED, "Invalid PreparedStatement handle");

        int resultSetScrollOpt = SCROLLOPT_FAST_FORWARD;
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorExecute) //
            .withTransactionDescriptor(transactionDescriptor) //

            // <prepared handle>
            // IN (reprepare): Old handle to unprepare before repreparing
            // OUT: The newly prepared handle
            .withParameter(RpcDirection.IN, preparedStatementHandle)
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, resultSetScrollOpt) // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0);// rowcount

        binding.forEach((name, encoded) -> {
            builder.withNamedParameter(RpcDirection.IN, name, encoded);
        });

        return builder.build();
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

        volatile boolean directMode;

        Phase phase = Phase.NONE;

        void update(Message it) {
            if (it instanceof RowToken) {
                hasSeenRows = true;
            }

            if (it instanceof ErrorToken) {
                hasSeenError = true;
            }
        }

        enum Phase {
            NONE, FETCHING, CLOSING, CLOSED, ERROR
        }
    }

    static class IntermediateCount extends AbstractDoneToken {

        public IntermediateCount(DoneInProcToken token) {
            super(token.getType(), token.getStatus(), token.getCurrentCommand(), token.getRowCount());
        }

        @Override
        public String getName() {
            return "INTERMEDIATE_COUNT";
        }
    }

    static class OnCursorComplete extends AtomicReference<Subscription> implements Runnable {

        private final Subscriber<?> upstream;

        OnCursorComplete(Subscriber<?> upstream) {
            this.upstream = upstream;
        }

        @Override
        public void run() {

            Subscription subscription = get();

            if (subscription != null) {
                subscription.cancel();
            }

            upstream.onComplete();
        }
    }
}
