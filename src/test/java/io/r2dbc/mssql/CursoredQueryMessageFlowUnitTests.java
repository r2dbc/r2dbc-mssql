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

import io.r2dbc.mssql.CursoredQueryMessageFlow.CursorState;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.token.AllHeaders;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.ClientMessageAssert;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.FluxSink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CursoredQueryMessageFlow}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
class CursoredQueryMessageFlowUnitTests {

    Client client = mock(Client.class);

    Runnable completion = mock(Runnable.class);

    FluxSink<ClientMessage> requests = mock(FluxSink.class);

    @BeforeEach
    void setUp() {
        when(client.getTransactionDescriptor()).thenReturn(TransactionDescriptor.empty());
    }
    
    @Test
    void shouldEncodeSpCursorOpen() {

        Collation collation = Collation.from(13632521, 52);

        RpcRequest rpcRequest = CursoredQueryMessageFlow.spCursorOpen("SELECT * FROM my_table", collation, TransactionDescriptor.empty());

        String hex = "FFFF020000000001260404000000000000E7" +
            "401F00D00409342C00530045004C0045" +
            "004300540020002A002000460052004F" +
            "004D0020006D0079005F007400610062" +
            "006C0065000000260404100000000000" +
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

        RpcRequest rpcRequest = CursoredQueryMessageFlow.spCursorFetch(180150003, CursoredQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty());

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

        RpcRequest rpcRequest = CursoredQueryMessageFlow.spCursorClose(180150003, TransactionDescriptor.empty());

        String hex = "FFFF090000000000260404F3DEBC0A";

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

        CursoredQueryMessageFlow.onDone(client, 128, requests, state, completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.FETCHING);
        verify(requests).next(CursoredQueryMessageFlow.spCursorFetch(state.cursorId, CursoredQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty()));
        verifyZeroInteractions(completion);
    }

    @Test
    void shouldContinueFetching() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.FETCHING;
        state.hasSeenRows = true;

        CursoredQueryMessageFlow.onDone(client, 128, requests, state, completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.FETCHING);
        verify(requests).next(CursoredQueryMessageFlow.spCursorFetch(state.cursorId, CursoredQueryMessageFlow.FETCH_NEXT, 128, TransactionDescriptor.empty()));
        verifyZeroInteractions(completion);
    }

    @Test
    void shouldStopFetching() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.FETCHING;

        CursoredQueryMessageFlow.onDone(client, 128, requests, state, completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSING);
        verify(requests).next(CursoredQueryMessageFlow.spCursorClose(state.cursorId, TransactionDescriptor.empty()));
        verifyZeroInteractions(completion);
    }

    @Test
    void shouldTransitionFromNoneToClosing() {

        CursorState state = new CursorState();
        state.cursorId = 42;

        CursoredQueryMessageFlow.onDone(client, 128, requests, state, completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSING);
        verify(requests).next(CursoredQueryMessageFlow.spCursorClose(state.cursorId, TransactionDescriptor.empty()));
        verifyZeroInteractions(completion);
    }

    @Test
    void shouldTransitionFromClosingToClosed() {

        CursorState state = new CursorState();
        state.cursorId = 42;
        state.phase = CursorState.Phase.CLOSING;

        CursoredQueryMessageFlow.onDone(client, 128, requests, state, completion);

        assertThat(state.phase).isEqualTo(CursorState.Phase.CLOSED);
        verifyZeroInteractions(requests);
        verify(completion).run();
    }
}
