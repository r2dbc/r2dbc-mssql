/*
 * Copyright 2025 the original author or authors.
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

package io.r2dbc.mssql.util;

import io.netty.util.ReferenceCounted;
import reactor.util.annotation.Nullable;

/**
 * Collection of methods to handle objects that may implement {@link ReferenceCounted}.
 *
 * @author Mark Paluch
 * @since 1.0.3
 */
public class ReferenceCountUtil {

    /**
     * Try to call {@link ReferenceCounted#release()} if the specified object implements {@link ReferenceCounted} and its reference count is greater than zero.
     * If the specified message doesn't implement {@link ReferenceCounted}, this method does nothing.
     */
    public static void maybeRelease(@Nullable Object obj) {
        if (obj instanceof ReferenceCounted && ((ReferenceCounted) obj).refCnt() > 0) {
            ((ReferenceCounted) obj).release();
        }
    }

    /**
     * Try to call {@link ReferenceCounted#release()} if the specified object implements {@link ReferenceCounted} and its reference count is greater than zero.
     * If the specified message doesn't implement {@link ReferenceCounted}, this method does nothing.
     */
    public static void maybeSafeRelease(@Nullable Object obj) {
        if (obj instanceof ReferenceCounted && ((ReferenceCounted) obj).refCnt() > 0) {
            io.netty.util.ReferenceCountUtil.safeRelease(obj);
        }
    }

    // Utility constructor
    private ReferenceCountUtil() {
    }
}
