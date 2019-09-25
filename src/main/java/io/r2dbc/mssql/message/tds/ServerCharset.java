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

package io.r2dbc.mssql.message.tds;

import java.nio.charset.Charset;

/**
 * Enumeration of encodings that are supported by SQL Server (and hopefully the JVM). See, for example,
 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html">https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html</a>
 * for a complete list of supported encodings with their canonical names.
 */
public enum ServerCharset {

    // @formatter:off
    UNICODE ("UTF-16LE", true, false),
    UTF8    ("UTF-8", true, false),
    CP437   ("Cp437", false, false),
    CP850   ("Cp850", false, false),
    CP874   ("MS874", true, true),
    CP932   ("MS932", true, false),
    CP936   ("MS936", true, false),
    CP949   ("MS949", true, false),
    CP950   ("MS950", true, false),
    CP1250  ("Cp1250", true, true),
    CP1251  ("Cp1251", true, true),
    CP1252  ("Cp1252", true, true),
    CP1253  ("Cp1253", true, true),
    CP1254  ("Cp1254", true, true),
    CP1255  ("Cp1255", true, true),
    CP1256  ("Cp1256", true, true),
    CP1257  ("Cp1257", true, true),
    CP1258  ("Cp1258", true, true);

    // @formatter:on

    private final String charsetName;

    private final boolean supportsAsciiConversion;

    private final boolean hasAsciiCompatibleSBCS;

    private boolean jvmSupportConfirmed;

    private Charset charset;

    ServerCharset(String charsetName, boolean supportsAsciiConversion, boolean hasAsciiCompatibleSBCS) {

        this.charsetName = charsetName;
        this.supportsAsciiConversion = supportsAsciiConversion;
        this.hasAsciiCompatibleSBCS = hasAsciiCompatibleSBCS;
    }

    /**
     * Lazily check codepage ({@link Charset}) support.
     *
     * @throws UnsupportedOperationException if the charset is not supported.
     */
    private void checkSupported() {

        if (!this.jvmSupportConfirmed) {
            // Checks for support by converting a java.lang.String
            // This works for all of the code pages above in SE 5 and later.
            if (!Charset.isSupported(this.charsetName)) {
                throw new UnsupportedOperationException(String.format("Code page not supported: %s", this.charsetName));
            }

            this.jvmSupportConfirmed = true;
        }
    }

    /**
     * Return the {@link Charset} for this encoding.
     *
     * @return the charset for this encoding.
     */
    public Charset charset() {

        checkSupported();

        if (this.charset == null) {
            this.charset = Charset.forName(this.charsetName);
        }

        return this.charset;
    }

    /**
     * @return {@code true} if the collation supports conversion to {@code true}.
     */
    public boolean supportsAsciiConversion() {
        return this.supportsAsciiConversion;
    }

    /**
     * @return {@code true} if the collation supports conversion to {@literal ascii} AND it uses a single-byte character set.
     */
    public boolean hasAsciiCompatibleSBCS() {
        return this.hasAsciiCompatibleSBCS;
    }
}
