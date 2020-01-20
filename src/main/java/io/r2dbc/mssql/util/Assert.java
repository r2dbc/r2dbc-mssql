/*
 * Copyright 2018-2020 the original author or authors.
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

import reactor.util.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Assertion utility class that assists in validating arguments.
 * <p>
 * Useful for identifying programmer errors early and clearly at runtime.
 * <p>
 * For example, if the contract of a public method states it does not allow {@code null} arguments, {@code Assert} can
 * be used to validate that contract. Doing this clearly indicates a contract violation when it occurs and protects the
 * class's invariants.
 * <p>
 * Typically used to validate method arguments rather than configuration properties, to check for cases that are usually
 * programmer errors rather than configuration errors. In contrast to configuration initialization code, there is
 * usually no point in falling back to defaults in such methods.
 * <p>
 * This class is similar to JUnit's assertion library. If an argument value is deemed invalid, an
 * {@link IllegalArgumentException} is thrown (typically). For example:
 *
 * <pre class="code">
 * Assert.notNull(clazz, "The class must not be null");
 * Assert.isTrue(i > 0, "The value must be greater than zero");
 * </pre>
 * <p>
 * Mainly for internal use within the framework; consider
 * <a href="https://commons.apache.org/proper/commons-lang/">Apache's Commons Lang</a> for a more comprehensive suite of
 * {@code String} utilities.
 *
 * @author Mark Paluch
 */
public final class Assert {

    /**
     * Assert a boolean expression, throwing an {@code IllegalStateException} if the expression evaluates to
     * {@code false}.
     * <p>
     * Call {@link #isTrue} if you wish to throw an {@code IllegalArgumentException} on an assertion failure.
     *
     * <pre class="code">
     * Assert.state(id == null, "The id property must not already be initialized");
     * </pre>
     *
     * @param expression a boolean expression
     * @param message    the exception message to use if the assertion fails
     * @throws IllegalStateException if {@code expression} is {@code false}
     */
    public static void state(boolean expression, String message) {
        if (!expression) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Assert a boolean expression, throwing an {@code IllegalStateException} if the expression evaluates to
     * {@code false}.
     * <p>
     * Call {@link #isTrue} if you wish to throw an {@code IllegalArgumentException} on an assertion failure.
     *
     * <pre class="code">
     * Assert.state(id == null, () -&gt; "ID for " + entity.getName() + " must not already be initialized");
     * </pre>
     *
     * @param expression      a boolean expression
     * @param messageSupplier a supplier for the exception message to use if the assertion fails
     * @throws IllegalStateException if {@code expression} is {@code false}
     */
    public static void state(boolean expression, Supplier<String> messageSupplier) {
        if (!expression) {
            throw new IllegalStateException(nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert a boolean expression, throwing an {@code IllegalArgumentException} if the expression evaluates to
     * {@code false}.
     *
     * <pre class="code">
     * Assert.isTrue(i &gt; 0, "The value must be greater than zero");
     * </pre>
     *
     * @param expression a boolean expression
     * @param message    the exception message to use if the assertion fails
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert a boolean expression, throwing an {@code IllegalArgumentException} if the expression evaluates to
     * {@code false}.
     *
     * <pre class="code">
     * Assert.isTrue(i &gt; 0, () -&gt; "The value '" + i + "' must be greater than zero");
     * </pre>
     *
     * @param expression      a boolean expression
     * @param messageSupplier a supplier for the exception message to use if the assertion fails
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void isTrue(boolean expression, Supplier<String> messageSupplier) {
        if (!expression) {
            throw new IllegalArgumentException(nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert that an object is {@code null}.
     *
     * <pre class="code">
     * Assert.isNull(value, "The value must be null");
     * </pre>
     *
     * @param object  the object to check
     * @param message the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is not {@code null}
     */
    public static void isNull(@Nullable Object object, String message) {
        if (object != null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert that an object is {@code null}.
     *
     * <pre class="code">
     * Assert.isNull(value, () -&gt; "The value '" + value + "' must be null");
     * </pre>
     *
     * @param object          the object to check
     * @param messageSupplier a supplier for the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is not {@code null}
     */
    public static void isNull(@Nullable Object object, Supplier<String> messageSupplier) {
        if (object != null) {
            throw new IllegalArgumentException(nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert that an object is not {@code null}.
     *
     * <pre class="code">
     * Assert.notNull(clazz, "The class must not be null");
     * </pre>
     *
     * @param object  the object to check
     * @param message the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static void notNull(@Nullable Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert that an object is not {@code null}.
     *
     * <pre class="code">
     * Assert.notNull(clazz, () -&gt; "The class '" + clazz.getName() + "' must not be null");
     * </pre>
     *
     * @param object          the object to check
     * @param messageSupplier a supplier for the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static void notNull(@Nullable Object object, Supplier<String> messageSupplier) {
        if (object == null) {
            throw new IllegalArgumentException(nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert that an object is not {@code null} and return the non-null instance.
     *
     * <pre class="code">
     * Class&lt?&gt; nonNullObject = Assert.requireNonNull(clazz, "The class must not be null");
     * </pre>
     *
     * @param object  the object to check
     * @param message the exception message to use if the assertion fails
     * @return the non-null {@code object}
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static <T> T requireNonNull(@Nullable T object, String message) {
        notNull(object, message);
        return object;
    }

    /**
     * Assert that an object is not {@code null} and return the non-null instance.
     *
     * <pre class="code">
     * Class&lt?&gt; nonNullObject = Assert.requireNonNull(clazz, () -&gt; "The class '" + clazz.getName() + "' must not be null");
     * </pre>
     *
     * @param object          the object to check
     * @param messageSupplier a supplier for the exception message to use if the assertion fails
     * @return the non-null {@code object}
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static <T> T requireNonNull(@Nullable T object, Supplier<String> messageSupplier) {
        notNull(object, messageSupplier);
        return object;
    }

    /**
     * Assert that the provided object is an instance of the provided class.
     * <pre class="code">Assert.instanceOf(Foo.class, foo, "Foo expected");</pre>
     *
     * @param type    the type to check against
     * @param obj     the object to check
     * @param message a message which will be prepended to provide further context.
     *                If it is empty or ends in ":" or ";" or "," or ".", a full exception message
     *                will be appended. If it ends in a space, the name of the offending object's
     *                type will be appended. In any other case, a ":" with a space and the name
     *                of the offending object's type will be appended.
     * @throws IllegalArgumentException if the object is not an instance of type
     */
    public static void isInstanceOf(Class<?> type, @Nullable Object obj, String message) {
        notNull(type, "Type to check against must not be null");
        if (!type.isInstance(obj)) {
            instanceCheckFailed(type, obj, message);
        }
    }

    /**
     * Assert that the provided object is an instance of the provided class.
     * <pre class="code">
     * Assert.instanceOf(Foo.class, foo, () -&gt; "Processing " + Foo.class.getSimpleName() + ":");
     * </pre>
     *
     * @param type            the type to check against
     * @param obj             the object to check
     * @param messageSupplier a supplier for the exception message to use if the
     *                        assertion fails. See {@link #isInstanceOf(Class, Object, String)} for details.
     * @throws IllegalArgumentException if the object is not an instance of type
     */
    public static void isInstanceOf(Class<?> type, @Nullable Object obj, Supplier<String> messageSupplier) {
        notNull(type, "Type to check against must not be null");
        if (!type.isInstance(obj)) {
            instanceCheckFailed(type, obj, nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert that the provided object is an instance of the provided class.
     * <pre class="code">Assert.instanceOf(Foo.class, foo);</pre>
     *
     * @param type the type to check against
     * @param obj  the object to check
     * @throws IllegalArgumentException if the object is not an instance of type
     */
    public static void isInstanceOf(Class<?> type, @Nullable Object obj) {
        isInstanceOf(type, obj, "");
    }

    /**
     * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
     * <pre class="code">Assert.isAssignable(Number.class, myClass, "Number expected");</pre>
     *
     * @param superType the super type to check against
     * @param subType   the sub type to check
     * @param message   a message which will be prepended to provide further context.
     *                  If it is empty or ends in ":" or ";" or "," or ".", a full exception message
     *                  will be appended. If it ends in a space, the name of the offending sub type
     *                  will be appended. In any other case, a ":" with a space and the name of the
     *                  offending sub type will be appended.
     * @throws IllegalArgumentException if the classes are not assignable
     */
    public static void isAssignable(Class<?> superType, @Nullable Class<?> subType, String message) {
        notNull(superType, "Super type to check against must not be null");
        if (subType == null || !superType.isAssignableFrom(subType)) {
            assignableCheckFailed(superType, subType, message);
        }
    }

    /**
     * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
     * <pre class="code">
     * Assert.isAssignable(Number.class, myClass, () -&gt; "Processing " + myAttributeName + ":");
     * </pre>
     *
     * @param superType       the super type to check against
     * @param subType         the sub type to check
     * @param messageSupplier a supplier for the exception message to use if the
     *                        assertion fails. See {@link #isAssignable(Class, Class, String)} for details.
     * @throws IllegalArgumentException if the classes are not assignable
     */
    public static void isAssignable(Class<?> superType, @Nullable Class<?> subType, Supplier<String> messageSupplier) {
        notNull(superType, "Super type to check against must not be null");
        if (subType == null || !superType.isAssignableFrom(subType)) {
            assignableCheckFailed(superType, subType, nullSafeGet(messageSupplier));
        }
    }

    /**
     * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
     * <pre class="code">Assert.isAssignable(Number.class, myClass);</pre>
     *
     * @param superType the super type to check
     * @param subType   the sub type to check
     * @throws IllegalArgumentException if the classes are not assignable
     */
    public static void isAssignable(Class<?> superType, Class<?> subType) {
        isAssignable(superType, subType, "");
    }

    private static void instanceCheckFailed(Class<?> type, @Nullable Object obj, @Nullable String msg) {
        String className = (obj != null ? obj.getClass().getName() : "null");
        String result = "";
        boolean defaultMessage = true;
        if (StringUtils.hasLength(msg)) {
            if (endsWithSeparator(msg)) {
                result = msg + " ";
            } else {
                result = messageWithTypeName(msg, className);
                defaultMessage = false;
            }
        }
        if (defaultMessage) {
            result = result + ("Object of class [" + className + "] must be an instance of " + type);
        }
        throw new IllegalArgumentException(result);
    }

    private static void assignableCheckFailed(Class<?> superType, @Nullable Class<?> subType, @Nullable String msg) {
        String result = "";
        boolean defaultMessage = true;
        if (StringUtils.hasLength(msg)) {
            if (endsWithSeparator(msg)) {
                result = msg + " ";
            } else {
                result = messageWithTypeName(msg, subType);
                defaultMessage = false;
            }
        }
        if (defaultMessage) {
            result = result + (subType + " is not assignable to " + superType);
        }
        throw new IllegalArgumentException(result);
    }

    private static boolean endsWithSeparator(String msg) {
        return (msg.endsWith(":") || msg.endsWith(";") || msg.endsWith(",") || msg.endsWith("."));
    }

    private static String messageWithTypeName(String msg, @Nullable Object typeName) {
        return msg + (msg.endsWith(" ") ? "" : ": ") + typeName;
    }

    @Nullable
    private static String nullSafeGet(@Nullable Supplier<String> messageSupplier) {
        return (messageSupplier != null ? messageSupplier.get() : null);
    }
}
