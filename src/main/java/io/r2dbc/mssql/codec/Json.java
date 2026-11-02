package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.mssql.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Value object to represent JSON values. The value is held as UTF-8 encoded bytes and can be consumed multiple times.
 * <p>Instances are created through the {@code of(...)} factory methods and consumed with {@link #asString()}, {@link #asArray()}
 * or one of the {@code map...(Function)} methods.
 */
public abstract class Json {

    Json() {
    }

    /**
     * Create a {@link Json} object from a {@link ByteBuffer}. The remaining bytes are copied.
     */
    public static Json of(ByteBuffer buffer) {

        Assert.requireNonNull(buffer, "buffer must not be null");

        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new JsonValue(bytes);
    }

    /**
     * Create a {@link Json} object from a {@link ByteBuf}. The readable bytes are copied and the buffer is released.
     */
    public static Json of(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "buffer must not be null");

        try {
            return new JsonValue(ByteBufUtil.getBytes(buffer));
        } finally {
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    /**
     * Create a {@link Json} object from an {@link InputStream}. The stream is read fully and closed.
     */
    public static Json of(InputStream inputStream) {

        Assert.requireNonNull(inputStream, "inputStream must not be null");

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            return new JsonValue(out.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read bytes from InputStream", e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Create a {@link Json} object from a {@code byte[]} (UTF-8 encoded JSON).
     */
    public static Json of(byte[] value) {

        Assert.requireNonNull(value, "value must not be null");

        return new JsonValue(value.clone());
    }

    /**
     * Create a {@link Json} object from a {@link String}.
     */
    public static Json of(String value) {

        Assert.requireNonNull(value, "value must not be null");

        return new JsonValue(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Apply the {@code mappingFunction} to the {@link ByteBuffer} representation of this JSON value.
     */
    public abstract <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction);

    /**
     * Apply the {@code mappingFunction} to the {@link ByteBuf} representation of this JSON value.
     */
    public abstract <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction);

    /**
     * Apply the {@code mappingFunction} to the {@link InputStream} representation of this JSON value.
     */
    public abstract <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction);

    /**
     * @return the value as UTF-8 encoded {@code byte[]}.
     */
    public abstract byte[] asArray();

    /**
     * @return the value as {@link String}.
     */
    public abstract String asString();

    /**
     * JSON value backed by a {@code byte[]}.
     */
    static final class JsonValue extends Json {

        private final byte[] value;

        JsonValue(byte[] value) {
            this.value = value;
        }

        @Override
        public <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(ByteBuffer.wrap(asArray()));
        }

        @Override
        public <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(Unpooled.wrappedBuffer(asArray()));
        }

        @Override
        public <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(new ByteArrayInputStream(this.value));
        }

        @Override
        public byte[] asArray() {
            return this.value.clone();
        }

        @Override
        public String asString() {
            return new String(this.value, StandardCharsets.UTF_8);
        }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof JsonValue && Arrays.equals(this.value, ((JsonValue) o).value));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(this.value);
        }

        @Override
        public String toString() {
            return "Json{" + asString() + '}';
        }
    }

}