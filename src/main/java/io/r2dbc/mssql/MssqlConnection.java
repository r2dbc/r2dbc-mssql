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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Connection} to a Microsoft SQL server.
 * 
 * @author Mark Paluch
 */
public class MssqlConnection implements Connection {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public MssqlConnection(Client client) {
		this.client = client;
	}

	private final Client client;

	@Override
	public Mono<Void> beginTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> close() {
		return this.client.close();
	}

	@Override
	public Mono<Void> commitTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Batch<?> createBatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> createSavepoint(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Statement<?> createStatement(String sql) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> releaseSavepoint(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> rollbackTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> rollbackTransactionToSavepoint(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
		throw new UnsupportedOperationException();
	}
}
