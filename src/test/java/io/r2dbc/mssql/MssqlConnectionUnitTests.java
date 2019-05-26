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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.client.TransactionStatus;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MssqlConnection}.
 *
 * @author Mark Paluch
 * @author Hebert Coelho
 */
class MssqlConnectionUnitTests {

    @Test
    void shouldBeginTransactionFromInitialState() {

        TestClient client =
            TestClient.builder().expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "SET IMPLICIT_TRANSACTIONS ON; BEGIN TRANSACTION")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldBeginTransactionFromExplicitState() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "BEGIN TRANSACTION")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotBeginTransactionFromStartedState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.STARTED);

        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldCommitFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "IF @@TRANCOUNT > 0 COMMIT TRANSACTION")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotCommitInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldRollbackFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotRollbackInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldNotSupportSavePointRelease() {

        Client clientMock = mock(Client.class);
        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        connection.releaseSavepoint("foo").as(StepVerifier::create).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "a", "A", "foo", "foo_bar"})
    void shouldAllowSavepointNames(String name) {

        Client clientMock = mock(Client.class);
        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        assertThat(connection.createSavepoint(name)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "@", "a\'", "a\"", "a[", "a]", "123456789012345678901234567890123"})
    void shouldRejectSavepointNames(String name) {

        Client clientMock = mock(Client.class);
        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        assertThatThrownBy(() -> connection.createSavepoint(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRollbackTransactionToSavepointFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "ROLLBACK TRANSACTION foo")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.rollbackTransactionToSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotRollbackTransactionToSavepointInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, new ConnectionOptions());

        connection.rollbackTransactionToSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldCreateSavepointFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "SET IMPLICIT_TRANSACTIONS ON; IF @@TRANCOUNT = 0 " +
                "BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN foo;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.createSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void createSavepoShouldBeginTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.AUTO_COMMIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "SET IMPLICIT_TRANSACTIONS ON; IF @@TRANCOUNT =" +
                " 0 BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN foo;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.createSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("isolationLevels")
    void shouldSetIsolationLevel(IsolationLevel isolationLevel) {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql().toUpperCase())).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, new ConnectionOptions());

        connection.setTransactionIsolationLevel(isolationLevel)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private static Stream<IsolationLevel> isolationLevels() {
        return Stream.of(MssqlIsolationLevel.SERIALIZABLE, MssqlIsolationLevel.READ_COMMITTED,
            MssqlIsolationLevel.READ_UNCOMMITTED, MssqlIsolationLevel.REPEATABLE_READ,
            MssqlIsolationLevel.SNAPSHOT);
    }
}
