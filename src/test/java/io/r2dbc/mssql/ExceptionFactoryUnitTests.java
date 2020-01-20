/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ExceptionFactory}.
 *
 * @author Mark Paluch
 */
class ExceptionFactoryUnitTests {

    @Test
    void shouldTranslateWellKnownGrammarErrors() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 130, 0x00, 0x10, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcBadGrammarException.class);
    }

    @Test
    void shouldTranslateWellKnownIntegrityViolation() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 2601, 0x00, 0x0E, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcDataIntegrityViolationException.class);
    }

    @Test
    void shouldTranslateGeneralPermissionErrors() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x0E, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcPermissionDeniedException.class);
    }

    @Test
    void shouldTranslateWellKnownTransientError() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 701, 0x00, 0x13, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcTransientException.class);
    }

    @Test
    void shouldTranslateGeneralGrammarErrors() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x0B, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcBadGrammarException.class);

        exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x0F, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcBadGrammarException.class);
    }

    @Test
    void shouldTranslateGeneralIntegrityViolation() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x0C, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcDataIntegrityViolationException.class);
    }

    @Test
    void shouldTranslateGeneralRollbackException() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x0D, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcRollbackException.class);
    }

    @Test
    void shouldTranslateGeneralError() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x10, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcNonTransientException.class);
    }

    @Test
    void shouldTranslateGeneralResourceException() {

        R2dbcException exception = ExceptionFactory.createException(new InfoToken(0, 999, 0x00, 0x11, "err", "", "", 0), "");

        assertThat(exception).isInstanceOf(R2dbcTransientResourceException.class);
    }
}
