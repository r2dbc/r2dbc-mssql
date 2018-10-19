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
package io.r2dbc.mssql.message.token;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.ByteBufUtils;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EnvChangeToken}.
 * 
 * @author Mark Paluch
 */
final class EnvChangeTokenUnitTests {

	@Test
	void shouldDecodeDatabaseChange() {

		ByteBuf buffer = ByteBufUtils.decodeHex("E31B0001066D0061007300740065007200066D0061007300740065007200");

		assertThat(buffer.readByte()).isEqualTo(EnvChangeToken.TYPE);

		EnvChangeToken token = EnvChangeToken.decode(buffer);

		assertThat(token.getChangeType()).isEqualTo(EnvChangeToken.EnvChangeType.Database);
		assertThat(token.getNewValueString()).isEqualTo("master");
		assertThat(token.getOldValueString()).isEqualTo("master");
	}

	@Test
	void shouldDecodeLanguageChange() {

		ByteBuf buffer = ByteBufUtils.decodeHex("e31700020a750073005f0065" + "006e0067006c0069007300680000");

		assertThat(buffer.readByte()).isEqualTo(EnvChangeToken.TYPE);

		EnvChangeToken token = EnvChangeToken.decode(buffer);

		assertThat(token.getChangeType()).isEqualTo(EnvChangeToken.EnvChangeType.Language);
		assertThat(token.getNewValueString()).isEqualTo("us_english");
		assertThat(token.getOldValueString()).isEqualTo("");
	}
}
