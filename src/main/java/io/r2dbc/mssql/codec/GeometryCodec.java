package io.r2dbc.mssql.codec;

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
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
 * @author svats0001
 */
public class GeometryCodec extends AbstractCodec<Geometry> {

    /**
     * Singleton instance.
     */
    static final GeometryCodec INSTANCE = new GeometryCodec();
    
    private GeometryCodec() {
        super(Geometry.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geometry value) {
        return BinaryCodec.INSTANCE.encode(allocator, context, value.serialize());
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.GEOMETRY;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return BinaryCodec.INSTANCE.encodeNull(allocator, serverType);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return BinaryCodec.INSTANCE.encodeNull(allocator);
    }
    
    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType().equals(SqlServerType.GEOMETRY);
    }

    @Nullable
    public Geometry decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Geometry> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            PlpLength plpLength = PlpLength.decode(buffer, decodable.getType());
            length = Length.of(Math.toIntExact(plpLength.getLength()), plpLength.isNull());
        } else {
            length = Length.decode(buffer, decodable.getType());
        }

        if (length.isNull()) {
            return null;
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

    @Override
    Geometry doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geometry> valueType) {

        if (length.isNull()) {
            return null;
        }

        byte[] geometryBytes = new byte[length.getLength()];

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            int dstIndex = 0;
            while (buffer.isReadable()) {
                int chunkLength = Length.decode(buffer, type).getLength();
                buffer.readBytes(geometryBytes, dstIndex, chunkLength);
                dstIndex += chunkLength;
            }

            try {
                return Geometry.deserialize(geometryBytes);
            } catch (SQLServerException exc) {
                return null;
            }
        }

        buffer.readBytes(geometryBytes);
        try {
            return Geometry.deserialize(geometryBytes);
        } catch (SQLServerException exc) {
            return null;
        }
    }
}
