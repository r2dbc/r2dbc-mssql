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
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.client.TransactionStatus;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
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

    static MssqlConnectionMetadata metadata = new MssqlConnectionMetadata("SQL Server", "1.0");

    static ConnectionOptions conectionOptions = new ConnectionOptions();

    @Test
    void shouldBeginTransactionFromInitialState() {

        TestClient client =
            TestClient.builder().expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "BEGIN TRANSACTION;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldBeginTransactionFromExplicitState() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "BEGIN TRANSACTION;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotBeginTransactionFromStartedState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.STARTED);

        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verify(clientMock, atLeast(1)).getContext();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldCommitFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "IF @@TRANCOUNT > 0 COMMIT TRANSACTION;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotCommitInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verify(clientMock, atLeast(1)).getContext();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldRollbackFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotRollbackInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verify(clientMock, atLeast(1)).getContext();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldNotSupportSavePointRelease() {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        connection.releaseSavepoint("foo").as(StepVerifier::create).verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "a", "A", "foo", "foo_bar"})
    void shouldAllowSavepointNames(String name) {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        assertThat(connection.createSavepoint(name)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "@", "a'", "a\"", "a[", "a]", "123456789012345678901234567890123"})
    void shouldRejectSavepointNames(String name) {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        assertThatThrownBy(() -> connection.createSavepoint(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRollbackTransactionToSavepointFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "ROLLBACK TRANSACTION foo")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.rollbackTransactionToSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldNotRollbackTransactionToSavepointInAutoCommitState() {

        Client clientMock = mock(Client.class);
        when(clientMock.getContext()).thenReturn(new ConnectionContext());
        when(clientMock.getTransactionStatus()).thenReturn(TransactionStatus.AUTO_COMMIT);

        MssqlConnection connection = new MssqlConnection(clientMock, metadata, conectionOptions);

        connection.rollbackTransactionToSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();

        verify(clientMock, times(2)).getTransactionStatus();
        verify(clientMock, atLeast(1)).getContext();
        verifyNoMoreInteractions(clientMock);
    }

    @Test
    void shouldCreateSavepointFromExplicitTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.STARTED).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "SET IMPLICIT_TRANSACTIONS ON; IF @@TRANCOUNT = 0 " +
                "BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN foo;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.createSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void createSavepointShouldBeginTransaction() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.AUTO_COMMIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "SET IMPLICIT_TRANSACTIONS ON; IF @@TRANCOUNT =" +
                " 0 BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN foo;")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

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

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.setTransactionIsolationLevel(isolationLevel)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldSetLockWaitTimeout() {

        TestClient client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SET LOCK_TIMEOUT 10000")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.setLockWaitTimeout(Duration.ofSeconds(10))
            .as(StepVerifier::create)
            .verifyComplete();

        client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SET LOCK_TIMEOUT -1")).thenRespond(DoneToken.create(0)).build();

        connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.setLockWaitTimeout(Duration.ofSeconds(-10))
            .as(StepVerifier::create)
            .verifyComplete();

        client =
            TestClient.builder().withTransactionStatus(TransactionStatus.EXPLICIT).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SET LOCK_TIMEOUT 0")).thenRespond(DoneToken.create(0)).build();

        connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.setLockWaitTimeout(Duration.ZERO)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void localValidationShouldValidateAgainstConnectionState() {

        TestClient connected =
            TestClient.builder().withConnected(true).build();

        MssqlConnection connection = new MssqlConnection(connected, metadata, conectionOptions);

        connection.validate(ValidationDepth.LOCAL)
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        TestClient disconnected =
            TestClient.builder().withConnected(false).build();

        connection = new MssqlConnection(disconnected, metadata, conectionOptions);

        connection.validate(ValidationDepth.LOCAL)
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void remoteValidationShouldIssueQuery() {

        TestClient client =
            TestClient.builder().withConnected(true).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SELECT 1")).thenRespond(DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.validate(ValidationDepth.REMOTE)
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void remoteValidationShouldFail() {

        TestClient client =
            TestClient.builder().withConnected(true).expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(),
                "SELECT 1")).thenRespond(new ErrorToken(1, 1, (byte) 1, (byte) 1, "failed", "", "", 0), DoneToken.create(0)).build();

        MssqlConnection connection = new MssqlConnection(client, metadata, conectionOptions);

        connection.validate(ValidationDepth.REMOTE)
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

    }

    @Nested
    class SanitizeTests {
        @Test
        void shorterThanMax() {
            assertThat(MssqlConnection.sanitize("12345", 10)).isEqualTo("12345");
        }

        @Test
        void exactlyMax() {
            assertThat(MssqlConnection.sanitize("1234567", 7)).isEqualTo("1234567");
        }

        @Test
        void greaterThanMax() {
            assertThat(MssqlConnection.sanitize("1234567", 3)).isEqualTo("567");
        }

        @Test
        void dropStartingPunctuation() {
            assertThat(MssqlConnection.sanitize("1_23_4", 5)).isEqualTo("23_4");
        }

    }

    private static Stream<IsolationLevel> isolationLevels() {
        return Stream.of(MssqlIsolationLevel.SERIALIZABLE, MssqlIsolationLevel.READ_COMMITTED,
            MssqlIsolationLevel.READ_UNCOMMITTED, MssqlIsolationLevel.REPEATABLE_READ,
            MssqlIsolationLevel.SNAPSHOT);
    }
}
