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
package io.r2dbc.mssql.util;

import java.nio.charset.Charset;

/**
 * Enumeration of encodings that are supported by SQL Server (and hopefully the JVM). See, for example,
 * https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html for a complete list of supported
 * encodings withtheir canonical names.
 */
public enum Encoding
{
    UNICODE ("UTF-16LE", true, false),
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

	private final String charsetName;
	private final boolean supportsAsciiConversion;
	private final boolean hasAsciiCompatibleSBCS;
	private boolean jvmSupportConfirmed = false;
	private Charset charset;

	Encoding(String charsetName, boolean supportsAsciiConversion, boolean hasAsciiCompatibleSBCS) {
		this.charsetName = charsetName;
		this.supportsAsciiConversion = supportsAsciiConversion;
		this.hasAsciiCompatibleSBCS = hasAsciiCompatibleSBCS;
	}

	final Encoding checkSupported() {

		if (!this.jvmSupportConfirmed) {
			// Checks for support by converting a java.lang.String
			// This works for all of the code pages above in SE 5 and later.
			if (!Charset.isSupported(this.charsetName)) {
				throw new UnsupportedOperationException(String.format("Code page %s not supported", this.charsetName));
			}

			this.jvmSupportConfirmed = true;
		}

		return this;
	}

	public Charset charset() {
		checkSupported();
		if (this.charset == null) {
			this.charset = Charset.forName(this.charsetName);
		}
		return this.charset;
	}

	/**
	 * Returns true if the collation supports conversion to ascii.
	 */
	private boolean supportsAsciiConversion() {
		return this.supportsAsciiConversion;
	}

	/**
	 * Returns true if the collation supports conversion to ascii AND it uses a single-byte character set.
	 */
	private boolean hasAsciiCompatibleSBCS() {
		return this.hasAsciiCompatibleSBCS;
	}
}
