/*
 * Copyright 2018-2021 the original author or authors.
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

import io.r2dbc.mssql.RpcQueryMessageFlow.CursorState;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.codec.RpcParameterContext;
import io.r2dbc.mssql.codec.RpcParameterContext.ValueContext;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.token.AllHeaders;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.ClientMessageAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RpcQueryMessageFlow}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
class RpcQueryMessageFlowUnitTests {

    Client client = mock(Client.class);

    Sinks.Many<ClientMessage> requests = mock(Sinks.Many.class);

    SynchronousSink<Message> sink = mock(SynchronousSink.class);

    Runnable completion = mock(Runnable.class);

    DefaultCodecs codecs = new DefaultCodecs();

    // windows-1252
    Collation collation = Collation.from(13632521, 52);

    @BeforeEach
    void setUp() {
        when(this.client.getTransactionDescriptor()).thenReturn(TransactionDescriptor.empty());
    }

    @Test
    void shouldEncodeSpExecuteSql() {

        Binding binding = new Binding();
        binding.add("P0", RpcDirection.IN, this.codecs.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(ValueContext.character(this.collation, true)), "mark"));

        RpcRequest rpcRequest = RpcQueryMessageFlow.spExecuteSql("SELECT * FROM my_table", binding, this.collation, TransactionDescriptor.empty());

        String hex = "ff ff 0a 00 00 00 00 00 e7 40" +
            "1f 09 04 d0 00 34 2c 00 53 00 45 00 4c 00 45 00" +
            "43 00 54 00 20 00 2a 00 20 00 46 00 52 00 4f 00" +
            "4d 00 20 00 6d 00 79 00 5f 00 74 00 61 00 62 00" +
            "6c 00 65 00 00 00 e7 40 1f 09 04 d0 00 34 24 00" +
            "40 00 50 00 30 00 20 00 6e 00 76 00 61 00 72 00" +
            "63 00 68 00 61 00 72 00 28 00 34 00 30 00 30 00" +
            "30 00 29 00 03 40 00 50 00 30 00 00 e7 40 1f 09" +
            "04 d0 00 34 08 00 6d 00 61 00 72 00 6b 00";

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldEncodeSpCursorOpen() {

        RpcRequest rpcRequest = RpcQueryMessageFlow.spCursorOpen("SELECT * FROM my_table", this.collation, TransactionDescriptor.empty());

        String hex = "FFFF020000000001260404000000000000E7" +
            "401F0904D000342C00530045004C0045" +
            "004300540020002A002000460052004F" +
            "004D0020006D0079005F007400610062" +
            "006C0065000000260404040000000000" +
            "26040401200000000126040400000000";

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldEncodeSpCursorFetch() {

        RpcRequest rpcRequest = RpcQueryMessageFlow.spCursorFetch(180150003, RpcQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty());

        String hex =
            "FFFF070002000000260404F3DEBC0A000026" +
                "04040200000000002604040000000000" +
                "0026040480000000";

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldEncodeSpCursorClose() {

        RpcRequest rpcRequest = RpcQueryMessageFlow.spCursorClose(180150003, TransactionDescriptor.empty());

        String hex = "FFFF090000000000260404F3DEBC0A";

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldEncodeSpPrepExec() {

        String hex = "ff ff 05 00 00 00 00 01 26 04" +
            "04 00 00 00 00 00 01 26 04 04 00 00 00 00 00 00" +
            "e7 40 1f 09 04 d0 00 34 24 00 40 00 50 00 30 00" +
            "20 00 6e 00 76 00 61 00 72 00 63 00 68 00 61 00" +
            "72 00 28 00 34 00 30 00 30 00 30 00 29 00 00 00" +
            "e7 40 1f 09 04 d0 00 34 48 00 55 00 50 00 44 00" +
            "41 00 54 00 45 00 20 00 6d 00 79 00 5f 00 74 00" +
            "61 00 62 00 6c 00 65 00 20 00 73 00 65 00 74 00" +
            "20 00 66 00 69 00 72 00 73 00 74 00 5f 00 6e 00" +
            "61 00 6d 00 65 00 20 00 3d 00 20 00 40 00 50 00" +
            "30 00 00 00 26 04 04 04 10 00 00 00 00 26 04 04" +
            "01 20 00 00 00 01 26 04 04 00 00 00 00 03 40 00" +
            "50 00 30 00 00 e7 40 1f 09 04 d0 00 34 08 00 6d" +
            "00 61 00 72 00 6b 00";

        String sql = "UPDATE my_table set first_name = @P0";

        Binding binding = new Binding();
        binding.add("P0", RpcDirection.IN, this.codecs.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(ValueContext.character(this.collation, true)), "mark"));

        RpcRequest rpcRequest = RpcQueryMessageFlow.spCursorPrepExec(0, sql, binding, this.collation, TransactionDescriptor.empty());

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldEncodeSpCursorExec() {

        String hex = "ff ff 04 00 00 00 00 00 26 04" +
            "04 02 00 00 00 00 01 26 04 04 00 00 00 00 00 00" +
            "26 04 04 04 00 00 00 00 00 26 04 04 01 20 00 00" +
            "00 01 26 04 04 00 00 00 00 03 40 00 50 00 30 00" +
            "00 e7 40 1f 09 04 d0 00 34 08 00 6d 00 61 00 72" +
            "00 6b 00";

        Binding binding = new Binding();
        binding.add("P0", RpcDirection.IN, this.codecs.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(ValueContext.character(this.collation, true)), "mark"));

        RpcRequest rpcRequest = RpcQueryMessageFlow.spCursorExec(2, binding, TransactionDescriptor.empty());

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                expected.writeBytes(HexUtils.decodeToByteBuf(hex));
            });
    }

    @Test
    void shouldTransitionFromNoneToFetching() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.hasMore = true;

        RpcQueryMessageFlow.onDone(this.client, 128, this.requests, state, this.completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.FETCHING);
        verify(this.requests).emitNext(RpcQueryMessageFlow.spCursorFetch(state.cursorId, RpcQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty()), Sinks.EmitFailureHandler.FAIL_FAST);
        verifyNoInteractions(this.completion);
    }

    @Test
    void shouldContinueFetching() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.FETCHING;
        state.hasSeenRows = true;

        RpcQueryMessageFlow.onDone(this.client, 128, this.requests, state, this.completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.FETCHING);
        verify(this.requests).emitNext(RpcQueryMessageFlow.spCursorFetch(state.cursorId, RpcQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty()), Sinks.EmitFailureHandler.FAIL_FAST);
        verifyNoInteractions(this.completion);
    }

    @Test
    void shouldStopFetching() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.FETCHING;

        RpcQueryMessageFlow.onDone(this.client, 128, this.requests, state, this.completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSING);
        verify(this.requests).emitNext(RpcQueryMessageFlow.spCursorClose(state.cursorId, TransactionDescriptor.empty()), Sinks.EmitFailureHandler.FAIL_FAST);
        verifyNoInteractions(this.sink);
    }

    @Test
    void shouldTransitionFromNoneToClosing() {

        CursorState state = new CursorState();
        state.cursorId = 42;

        RpcQueryMessageFlow.onDone(this.client, 128, this.requests, state, this.completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSING);
        verify(this.requests).emitNext(RpcQueryMessageFlow.spCursorClose(state.cursorId, TransactionDescriptor.empty()), Sinks.EmitFailureHandler.FAIL_FAST);
        verifyNoInteractions(this.completion);
    }

    @Test
    void shouldTransitionFromClosingToClosed() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.CLOSING;

        RpcQueryMessageFlow.onDone(this.client, 128, this.requests, state, this.completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSED);
        verifyNoInteractions(this.requests);
        verify(this.completion).run();
    }
}
