/*
 * Copyright 2019 the original author or authors.
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

import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;

/**
 * {@link Comparator} for column name  ({@code sysname}) comparison. Uses {@link Collator#SECONDARY lenient} comparison by default.
 * Supports name escaping with square brackets ({@code [sysname]}) to enforce exact comparison rules.
 *
 * @author Mark Paluch
 */
enum EscapeAwareComparator implements Comparator<String> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    static final Collator LENIENT;

    static final Collator EXACT;

    static {
        Collator lenient = Collator.getInstance(Locale.US);
        lenient.setStrength(Collator.SECONDARY);
        LENIENT = lenient;

        Collator exact = Collator.getInstance(Locale.US);
        exact.setStrength(Collator.IDENTICAL);
        EXACT = exact;
    }

    @Override
    public int compare(String o1, String o2) {

        boolean exactMatch = false;

        if (o1.startsWith("[") && o1.endsWith("]")) {
            exactMatch = true;
            o1 = o1.substring(1, o1.length() - 1);
        }

        if (o2.startsWith("[") && o2.endsWith("]")) {
            exactMatch = true;
            o2 = o2.substring(1, o2.length() - 1);
        }

        return exactMatch ? EXACT.compare(o1, o2) : LENIENT.compare(o1, o2);
    }
}
