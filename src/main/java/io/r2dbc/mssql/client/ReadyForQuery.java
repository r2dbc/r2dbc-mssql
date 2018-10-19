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
package io.r2dbc.mssql.client;

import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.Tabular;

/**
 * Synthetic message indicates the connection is ready for query.
 * 
 * @author Mark Paluch
 */
class ReadyForQuery implements Message {

	private final Tabular loginack;

	public ReadyForQuery(Tabular loginack) {
		this.loginack = loginack;
	}
}
