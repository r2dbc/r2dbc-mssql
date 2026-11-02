package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import reactor.util.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Codec for JSON values that are represented as {@link Json}. Delegates to {@link StringCodec}.
 */
final class JsonCodec extends AbstractCodec<Json> {

    static final JsonCodec INSTANCE = new JsonCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.CHAR, SqlServerType.NCHAR,
            SqlServerType.VARCHAR, SqlServerType.NVARCHAR,
            SqlServerType.VARCHARMAX, SqlServerType.NVARCHARMAX,
            SqlServerType.TEXT, SqlServerType.NTEXT);

    private JsonCodec() {
        super(Json.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Json value) {
        return StringCodec.INSTANCE.doEncode(allocator, context, value.asString());
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return false; // StringCodec and ClobCodec already handle typed nulls
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return StringCodec.INSTANCE.doEncodeNull(allocator);
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return StringCodec.INSTANCE.encodeNull(allocator, serverType);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Override
    @Nullable
    public Json decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Json> type) {
        String value = StringCodec.INSTANCE.decode(buffer, decodable, String.class);
        return value == null ? null : Json.of(value);
    }

    @Override
    @Nullable
    Json doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Json> valueType) {
        String value = StringCodec.INSTANCE.doDecode(buffer, length, type, String.class);
        return value == null ? null : Json.of(value);
    }
}