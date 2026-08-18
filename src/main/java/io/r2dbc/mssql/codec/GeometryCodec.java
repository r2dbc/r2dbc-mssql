package io.r2dbc.mssql.codec;

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import reactor.util.annotation.Nullable;

/**
 * Codec for date types that are represented as {@link Geometry}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#GEOMETRY}</li>
 * <li>Java type: {@link Geometry}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @since 1.0.6
 */
final class GeometryCodec extends GeospatialCodecSupport<Geometry> {

    /**
     * Singleton instance.
     */
    static final GeometryCodec INSTANCE = new GeometryCodec();

    private GeometryCodec() {
        super(Geometry.class, SqlServerType.GEOMETRY);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geometry value) {
        return SpatialDatatypeEncoded.encode(allocator, SqlServerType.GEOMETRY, value.serialize());
    }

    @Override
    @Nullable
    Geometry doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geometry> valueType) {

        if (length.isNull()) {
            return null;
        }

        try {
            byte[] geometryBytes = Decode.readBytesOrPlp(buffer, length, type);
            return Geometry.deserialize(geometryBytes);
        } catch (SQLServerException exc) {
            throw new SpatialDatatypeDecodeException("Cannot decode geometry data", exc);
        }
    }

}
