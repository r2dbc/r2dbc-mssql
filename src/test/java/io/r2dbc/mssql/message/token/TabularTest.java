package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TabularTest {
    private Tabular.TabularDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder=Tabular.createDecoder(false);
    }

    @AfterEach
    void tearDown() {
        decoder=null;
    }

    @Test
    void decode() {
        ByteBuf data = HexUtils.decodeToByteBuf(
                //"54e1ad4439a1c46413e38ec308004500" +
                //"0133194640007b066f590a6419330a6c" +
                //"48230599d77d59791eda7ef2c0225018" +
                //"01ff3ab400000401010b00340100"+
                "8105" +
                "00000000001000380249004400000000" +
                "00090026040850006100720065006e00" +
                "740049004400000000000900e7900109" +
                "04d00034044e0061006d006500000000" +
                "000900e7a00f0904d000340e43006100" +
                "6c00630075006c006100740065006400" +
                "50006100740068000000000000003807" +
                "52004f0057005300540041005400a42b" +
                "00020300640062006f00100079004100" +
                "63007400690076006900740079004f00" +
                "7500740063006f006d006500a50f0001" +
                "0100020100030100040100050014ff11" +
                "00c10000000000000000007900000000" +
                "ac00000001000000000000260404f3de" +
                "bc0aac04000001000000000000260404" +
                "fffffffffe0000e00000000000000000" + "00");
        decoder.decode(data);
    }
}