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

package io.r2dbc.mssql.client.ssl;

import io.r2dbc.mssql.util.Assert;

import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Hostname matcher. Accepts either a simple, fully-qualified or hostname with wildcards to match against SSL certificate hostnames.
 *
 * <ul>
 * <li>{@code foo} matches {@code foo} but not {@code bar}</li>
 * <li>{@code foo.bar} matches {@code foo.bar} but not {@code foo.baz}</li>
 * <li>{@code *.bar} matches {@code foo.bar} but not {@code foo.baz}</li>
 * <li>{@code f**.bar} matches {@code foo.bar} but not {@code boo.bar}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
class HostNamePredicate implements Predicate<String> {

    private final Pattern hostnamePattern;

    private HostNamePredicate(Pattern hostnamePattern) {
        this.hostnamePattern = hostnamePattern;
    }

    /**
     * Create a new {@link HostNamePredicate} instance.
     *
     * @param expectedHostname expected host name.
     * @return the {@link HostNamePredicate}.
     * @throws IllegalArgumentException if {@code expectedHostname} is {@code null}.
     */
    public static HostNamePredicate of(String expectedHostname) {

        Assert.notNull(expectedHostname, "Expected hostname must not be null");

        StringBuilder builder = new StringBuilder();

        // canonicalize to lower-case
        String[] segments = expectedHostname.toLowerCase(Locale.ENGLISH).split("\\.");

        for (String segment : segments) {

            if (builder.length() != 0) {
                builder.append("\\.");
            }

            StringBuilder rewrittenSegment = new StringBuilder(segment.length());
            StringBuilder part = new StringBuilder(segment.length());

            for (char c : segment.toCharArray()) {

                if (c == '*') {

                    if (part.length() != 0) {
                        rewrittenSegment.append(Pattern.quote(part.toString()));
                        part = new StringBuilder(segment.length());
                    }
                    rewrittenSegment.append("([^\\.]*)");
                } else {
                    part.append(c);
                }
            }

            if (part.length() != 0) {
                rewrittenSegment.append(Pattern.quote(part.toString()));
            }

            builder.append(rewrittenSegment);
        }

        return new HostNamePredicate(Pattern.compile(builder.toString()));
    }

    @Override
    public boolean test(String s) {
        return this.hostnamePattern.matcher(s).matches();
    }
}
