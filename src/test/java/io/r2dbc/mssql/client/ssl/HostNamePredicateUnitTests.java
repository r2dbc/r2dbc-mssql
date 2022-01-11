/*
 * Copyright 2019-2022 the original author or authors.
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

package io.r2dbc.mssql.client.ssl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HostNamePredicate}.
 *
 * @author Mark Paluch
 */
class HostNamePredicateUnitTests {

    @Test
    void shouldRejectAll() {
        assertThat(HostNamePredicate.of("")).rejects("bar");
    }

    @Test
    void shouldMatchSimpleName() {
        assertThat(HostNamePredicate.of("foo")).accepts("foo").rejects("bar");
    }

    @Test
    void shouldMatchNameWithDots() {
        assertThat(HostNamePredicate.of("foo.bar.baz")).accepts("foo.bar.baz").rejects("bar").rejects("baz.bar.foo");
    }

    @Test
    void shouldMatchWildcard() {
        assertThat(HostNamePredicate.of("foo.*.baz")).accepts("foo.bar.baz").accepts("foo..baz").accepts("foo.bar.baz").rejects("foo.baz").rejects("baz.bar.foo");
    }

    @Test
    void shouldMatchWildcardInWord() {
        assertThat(HostNamePredicate.of("foo.a*z.baz")).accepts("foo.agz.baz").accepts("foo.az.baz").rejects("foo.bar.baz");
    }

    @Test
    void shouldMatchWildcardInWildcardCertificate() {
        assertThat(HostNamePredicate.of("*.foo.bar.net")).accepts("*.foo.bar.net").rejects("*.foo.bar.baz").rejects("*.subdomain.foo.bar.net");
    }
}
