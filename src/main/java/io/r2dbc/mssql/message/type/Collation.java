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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.util.Assert;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A collation represents encoding and collation used for character data types. Collation is in the following BNF format
 * (see TDS spec for full details):
 * <ul>
 *     <li>LCID := 20 BIT</li>
 *     <li>fIgnoreCase := BIT</li>
 *     <li>fIgnoreAccent := BIT</li>
 *     <li>fIgnoreWidth := BIT</li>
 *     <li>fIgnoreKana := BIT</li>
 *     <li>fBinary := BIT</li>
 *     <li>ColFlags := fIgnoreCase, fIgnoreAccent, fIgnoreWidth, fIgnoreKana, fBinary, FRESERVEDBIT, FRESERVEDBIT, FRESERVEDBIT</li>
 *     <li>Version := 4 BIT</li>
 *     <li>SortId := BYTE</li>
 * </ul>
 *
 * @author Mark Paluch
 * @see ServerCharset
 */
@SuppressWarnings("unused")
public final class Collation {

    private static final LongObjectMap<Collation> COLLATIONS = new LongObjectHashMap<>();

    public static final Collation RAW = Collation.from(0, 0);

    private static final int UTF8_IN_TDSCOLLATION = 0x4000000;

    // Index from of windows locales by their LangIDs for fast lookup
    // of encodings associated with various SQL collations
    private static final Map<Integer, WindowsLocale> localeCache;

    private static final Map<Integer, SortOrder> sortOrderCache;

    static {

        // Populate the windows locale and sort order indices

        localeCache = new HashMap<>();
        for (WindowsLocale locale : WindowsLocale.values()) {
            localeCache.put(locale.langID, locale);
        }

        sortOrderCache = new HashMap<>();

        for (SortOrder sortOrder : SortOrder.values()) {
            sortOrderCache.put(sortOrder.sortId, sortOrder);
        }
    }

    private final int lcid; // First 4 bytes of TDS collation.

    private final int sortId; // 5th byte of TDS collation.

    private final ServerCharset serverCharset;

    private Collation(int lcid, int sortId) throws UnsupportedEncodingException {

        this.lcid = lcid;
        this.sortId = sortId;

        if (lcid != 0 || sortId != 0) {

            if (UTF8_IN_TDSCOLLATION == (lcid & UTF8_IN_TDSCOLLATION)) {
                this.serverCharset = ServerCharset.UTF8;
            } else {
                // For a SortId==0 collation, the LCID bits correspond to a LocaleId
                this.serverCharset = (0 == sortId) ? getEncodingFromLCID() : getEncodingFromSortId();
            }
        } else {
            this.serverCharset = ServerCharset.CP1252;
        }
    }

    /**
     * Create a new {@link Collation} from LCID and {@code sortId}.
     *
     * @param lcid   locale Id
     * @param sortId sort Id
     * @return the {@link Collation}.
     */
    public static Collation from(int lcid, int sortId) {

        Collation collation;
        long cacheKey = lcid | (sortId << 4);

        synchronized (COLLATIONS) {

            collation = COLLATIONS.get(cacheKey);
            if (collation == null) {

                try {
                    collation = new Collation(lcid, sortId);
                } catch (UnsupportedEncodingException e) {
                    throw ProtocolException.unsupported(e);
                }

                COLLATIONS.put(cacheKey, collation);
            }

            return collation;
        }
    }

    /**
     * Decode the {@link Collation}.
     *
     * @param buffer the buffer.
     * @return the decoded Collation.
     */
    public static Collation decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Buffer must not be null");

        int info = Decode.asInt(buffer); // 4 bytes, contains: LCID ColFlags Version
        int sortId = Decode.uByte(buffer); // 1 byte, contains: SortId

        return Collation.from(info, sortId);
    }

    static int getLength() {
        return 5;
    } // Length of collation in TDS (in bytes)

    /**
     * Encode the collation.
     *
     * @param buffer the data buffer.
     */
    public void encode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        Encode.asInt(buffer, this.lcid);
        Encode.asByte(buffer, (byte) this.sortId);
    }

    // Utility methods for getting details of this collation's encoding
    public Charset getCharset() {
        return this.serverCharset.charset();
    }

    /**
     * Returns the collation info.
     *
     * @return the LCID (collation info).
     */
    int getLCID() {
        return this.lcid;
    }

    /**
     * Returns the  sort Id
     *
     * @return the sort Od.
     */
    int getSortId() {
        return this.sortId;
    }

    /**
     * Returns whether the underlying encoding supports ASCII conversion.
     *
     * @return {@code true} if the underlying encoding supports ASCII conversion.
     */
    boolean supportsAsciiConversion() {
        return this.serverCharset.supportsAsciiConversion();
    }

    /**
     * Returns whether the underlying encoding allows fast-path ASCII conversion by filtering lower ASCII chars.
     *
     * @return {@code true} the underlying encoding allows fast-path ASCII.
     */
    boolean hasAsciiCompatibleSBCS() {
        return this.serverCharset.hasAsciiCompatibleSBCS();
    }

    private int getLanguageId() {
        return this.lcid & 0x0000FFFF;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Collation)) {
            return false;
        }
        Collation collation = (Collation) o;
        return this.lcid == collation.lcid &&
            this.sortId == collation.sortId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.lcid, this.sortId);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [encoding=").append(this.serverCharset);
        sb.append(']');
        return sb.toString();
    }

    private ServerCharset getEncodingFromLCID() throws UnsupportedEncodingException {

        WindowsLocale locale = localeCache.get(getLanguageId());

        if (locale == null) {
            throw new UnsupportedEncodingException(String.format("Windows collation not supported: %s", Integer.toHexString(getLanguageId()).toUpperCase()));
        }

        try {
            return locale.getServerCharset();
        } catch (RuntimeException inner) {

            UnsupportedEncodingException e = new UnsupportedEncodingException(String.format("Windows collation not supported: %s",
                Integer.toHexString(getLanguageId()).toUpperCase()));
            e.initCause(inner);

            throw e;
        }
    }

    private ServerCharset getEncodingFromSortId() throws UnsupportedEncodingException {

        SortOrder sortOrder = sortOrderCache.get(this.sortId);

        if (sortOrder == null) {
            throw new UnsupportedEncodingException(String.format("SQL Server collation is not supported: %d", this.sortId));
        }

        try {
            return sortOrder.getServerCharset();
        } catch (RuntimeException inner) {

            UnsupportedEncodingException e = new UnsupportedEncodingException(String.format("SQL Server collation is not supported: %d", this.sortId));
            e.initCause(inner);
            throw e;
        }
    }

    /**
     * Enumeration of Windows locales recognized by SQL Server.
     * <p/>
     * For our purposes in the driver, locales are only described by their LangID and character encodings.
     * The set of locales is derived from the following resources:
     * https://download.microsoft.com/download/9/5/e/95ef66af-9026-4bb0-a41d-a4f81802d92c/[MS-LCID].pdf Lists LCID values
     * and their corresponding meanings (in RFC 3066 format). Used to derive the names for the various enumeration
     * constants.
     * <p/>
     * Collectively, these two
     * tables provide a mapping of collation-version specific encodings for every locale supported by SQL Server. Lang
     * IDs are derived from locales' LCIDs.
     */
    enum WindowsLocale {
        ar_SA(0x0401, ServerCharset.CP1256),
        bg_BG(0x0402, ServerCharset.CP1251),
        ca_ES(0x0403, ServerCharset.CP1252),
        zh_TW(0x0404, ServerCharset.CP950),
        cs_CZ(0x0405, ServerCharset.CP1250),
        da_DK(0x0406, ServerCharset.CP1252),
        de_DE(0x0407, ServerCharset.CP1252),
        el_GR(0x0408, ServerCharset.CP1253),
        en_US(0x0409, ServerCharset.CP1252),
        es_ES_tradnl(0x040a, ServerCharset.CP1252),
        fi_FI(0x040b, ServerCharset.CP1252),
        fr_FR(0x040c, ServerCharset.CP1252),
        he_IL(0x040d, ServerCharset.CP1255),
        hu_HU(0x040e, ServerCharset.CP1250),
        is_IS(0x040f, ServerCharset.CP1252),
        it_IT(0x0410, ServerCharset.CP1252),
        ja_JP(0x0411, ServerCharset.CP932),
        ko_KR(0x0412, ServerCharset.CP949),
        nl_NL(0x0413, ServerCharset.CP1252),
        nb_NO(0x0414, ServerCharset.CP1252),
        pl_PL(0x0415, ServerCharset.CP1250),
        pt_BR(0x0416, ServerCharset.CP1252),
        rm_CH(0x0417, ServerCharset.CP1252),
        ro_RO(0x0418, ServerCharset.CP1250),
        ru_RU(0x0419, ServerCharset.CP1251),
        hr_HR(0x041a, ServerCharset.CP1250),
        sk_SK(0x041b, ServerCharset.CP1250),
        sq_AL(0x041c, ServerCharset.CP1250),
        sv_SE(0x041d, ServerCharset.CP1252),
        th_TH(0x041e, ServerCharset.CP874),
        tr_TR(0x041f, ServerCharset.CP1254),
        ur_PK(0x0420, ServerCharset.CP1256),
        id_ID(0x0421, ServerCharset.CP1252),
        uk_UA(0x0422, ServerCharset.CP1251),
        be_BY(0x0423, ServerCharset.CP1251),
        sl_SI(0x0424, ServerCharset.CP1250),
        et_EE(0x0425, ServerCharset.CP1257),
        lv_LV(0x0426, ServerCharset.CP1257),
        lt_LT(0x0427, ServerCharset.CP1257),
        tg_Cyrl_TJ(0x0428, ServerCharset.CP1251),
        fa_IR(0x0429, ServerCharset.CP1256),
        vi_VN(0x042a, ServerCharset.CP1258),
        hy_AM(0x042b, ServerCharset.CP1252),
        az_Latn_AZ(0x042c, ServerCharset.CP1254),
        eu_ES(0x042d, ServerCharset.CP1252),
        wen_DE(0x042e, ServerCharset.CP1252),
        mk_MK(0x042f, ServerCharset.CP1251),
        tn_ZA(0x0432, ServerCharset.CP1252),
        xh_ZA(0x0434, ServerCharset.CP1252),
        zu_ZA(0x0435, ServerCharset.CP1252),
        Af_ZA(0x0436, ServerCharset.CP1252),
        ka_GE(0x0437, ServerCharset.CP1252),
        fo_FO(0x0438, ServerCharset.CP1252),
        hi_IN(0x0439, ServerCharset.UNICODE),
        mt_MT(0x043a, ServerCharset.UNICODE),
        se_NO(0x043b, ServerCharset.CP1252),
        ms_MY(0x043e, ServerCharset.CP1252),
        kk_KZ(0x043f, ServerCharset.CP1251),
        ky_KG(0x0440, ServerCharset.CP1251),
        sw_KE(0x0441, ServerCharset.CP1252),
        tk_TM(0x0442, ServerCharset.CP1250),
        uz_Latn_UZ(0x0443, ServerCharset.CP1254),
        tt_RU(0x0444, ServerCharset.CP1251),
        bn_IN(0x0445, ServerCharset.UNICODE),
        pa_IN(0x0446, ServerCharset.UNICODE),
        gu_IN(0x0447, ServerCharset.UNICODE),
        or_IN(0x0448, ServerCharset.UNICODE),
        ta_IN(0x0449, ServerCharset.UNICODE),
        te_IN(0x044a, ServerCharset.UNICODE),
        kn_IN(0x044b, ServerCharset.UNICODE),
        ml_IN(0x044c, ServerCharset.UNICODE),
        as_IN(0x044d, ServerCharset.UNICODE),
        mr_IN(0x044e, ServerCharset.UNICODE),
        sa_IN(0x044f, ServerCharset.UNICODE),
        mn_MN(0x0450, ServerCharset.CP1251),
        bo_CN(0x0451, ServerCharset.UNICODE),
        cy_GB(0x0452, ServerCharset.CP1252),
        km_KH(0x0453, ServerCharset.UNICODE),
        lo_LA(0x0454, ServerCharset.UNICODE),
        gl_ES(0x0456, ServerCharset.CP1252),
        kok_IN(0x0457, ServerCharset.UNICODE),
        syr_SY(0x045a, ServerCharset.UNICODE),
        si_LK(0x045b, ServerCharset.UNICODE),
        iu_Cans_CA(0x045d, ServerCharset.CP1252),
        am_ET(0x045e, ServerCharset.CP1252),
        ne_NP(0x0461, ServerCharset.UNICODE),
        fy_NL(0x0462, ServerCharset.CP1252),
        ps_AF(0x0463, ServerCharset.UNICODE),
        fil_PH(0x0464, ServerCharset.CP1252),
        dv_MV(0x0465, ServerCharset.UNICODE),
        ha_Latn_NG(0x0468, ServerCharset.CP1252),
        yo_NG(0x046a, ServerCharset.CP1252),
        quz_BO(0x046b, ServerCharset.CP1252),
        nso_ZA(0x046c, ServerCharset.CP1252),
        ba_RU(0x046d, ServerCharset.CP1251),
        lb_LU(0x046e, ServerCharset.CP1252),
        kl_GL(0x046f, ServerCharset.CP1252),
        ig_NG(0x0470, ServerCharset.CP1252),
        ii_CN(0x0478, ServerCharset.CP1252),
        arn_CL(0x047a, ServerCharset.CP1252),
        moh_CA(0x047c, ServerCharset.CP1252),
        br_FR(0x047e, ServerCharset.CP1252),
        ug_CN(0x0480, ServerCharset.CP1256),
        mi_NZ(0x0481, ServerCharset.UNICODE),
        oc_FR(0x0482, ServerCharset.CP1252),
        co_FR(0x0483, ServerCharset.CP1252),
        gsw_FR(0x0484, ServerCharset.CP1252),
        sah_RU(0x0485, ServerCharset.CP1251),
        qut_GT(0x0486, ServerCharset.CP1252),
        rw_RW(0x0487, ServerCharset.CP1252),
        wo_SN(0x0488, ServerCharset.CP1252),
        prs_AF(0x048c, ServerCharset.CP1256),
        ar_IQ(0x0801, ServerCharset.CP1256),
        zh_CN(0x0804, ServerCharset.CP936),
        de_CH(0x0807, ServerCharset.CP1252),
        en_GB(0x0809, ServerCharset.CP1252),
        es_MX(0x080a, ServerCharset.CP1252),
        fr_BE(0x080c, ServerCharset.CP1252),
        it_CH(0x0810, ServerCharset.CP1252),
        nl_BE(0x0813, ServerCharset.CP1252),
        nn_NO(0x0814, ServerCharset.CP1252),
        pt_PT(0x0816, ServerCharset.CP1252),
        sr_Latn_CS(0x081a, ServerCharset.CP1250),
        sv_FI(0x081d, ServerCharset.CP1252),
        Lithuanian_Classic(0x0827, ServerCharset.CP1257),
        az_Cyrl_AZ(0x082c, ServerCharset.CP1251),
        dsb_DE(0x082e, ServerCharset.CP1252),
        se_SE(0x083b, ServerCharset.CP1252),
        ga_IE(0x083c, ServerCharset.CP1252),
        ms_BN(0x083e, ServerCharset.CP1252),
        uz_Cyrl_UZ(0x0843, ServerCharset.CP1251),
        bn_BD(0x0845, ServerCharset.UNICODE),
        mn_Mong_CN(0x0850, ServerCharset.CP1251),
        iu_Latn_CA(0x085d, ServerCharset.CP1252),
        tzm_Latn_DZ(0x085f, ServerCharset.CP1252),
        quz_EC(0x086b, ServerCharset.CP1252),
        ar_EG(0x0c01, ServerCharset.CP1256),
        zh_HK(0x0c04, ServerCharset.CP950),
        de_AT(0x0c07, ServerCharset.CP1252),
        en_AU(0x0c09, ServerCharset.CP1252),
        es_ES(0x0c0a, ServerCharset.CP1252),
        fr_CA(0x0c0c, ServerCharset.CP1252),
        sr_Cyrl_CS(0x0c1a, ServerCharset.CP1251),
        se_FI(0x0c3b, ServerCharset.CP1252),
        quz_PE(0x0c6b, ServerCharset.CP1252),
        ar_LY(0x1001, ServerCharset.CP1256),
        zh_SG(0x1004, ServerCharset.CP936),
        de_LU(0x1007, ServerCharset.CP1252),
        en_CA(0x1009, ServerCharset.CP1252),
        es_GT(0x100a, ServerCharset.CP1252),
        fr_CH(0x100c, ServerCharset.CP1252),
        hr_BA(0x101a, ServerCharset.CP1250),
        smj_NO(0x103b, ServerCharset.CP1252),
        ar_DZ(0x1401, ServerCharset.CP1256),
        zh_MO(0x1404, ServerCharset.CP950),
        de_LI(0x1407, ServerCharset.CP1252),
        en_NZ(0x1409, ServerCharset.CP1252),
        es_CR(0x140a, ServerCharset.CP1252),
        fr_LU(0x140c, ServerCharset.CP1252),
        bs_Latn_BA(0x141a, ServerCharset.CP1250),
        smj_SE(0x143b, ServerCharset.CP1252),
        ar_MA(0x1801, ServerCharset.CP1256),
        en_IE(0x1809, ServerCharset.CP1252),
        es_PA(0x180a, ServerCharset.CP1252),
        fr_MC(0x180c, ServerCharset.CP1252),
        sr_Latn_BA(0x181a, ServerCharset.CP1250),
        sma_NO(0x183b, ServerCharset.CP1252),
        ar_TN(0x1c01, ServerCharset.CP1256),
        en_ZA(0x1c09, ServerCharset.CP1252),
        es_DO(0x1c0a, ServerCharset.CP1252),
        sr_Cyrl_BA(0x1c1a, ServerCharset.CP1251),
        sma_SB(0x1c3b, ServerCharset.CP1252),
        ar_OM(0x2001, ServerCharset.CP1256),
        en_JM(0x2009, ServerCharset.CP1252),
        es_VE(0x200a, ServerCharset.CP1252),
        bs_Cyrl_BA(0x201a, ServerCharset.CP1251),
        sms_FI(0x203b, ServerCharset.CP1252),
        ar_YE(0x2401, ServerCharset.CP1256),
        en_CB(0x2409, ServerCharset.CP1252),
        es_CO(0x240a, ServerCharset.CP1252),
        smn_FI(0x243b, ServerCharset.CP1252),
        ar_SY(0x2801, ServerCharset.CP1256),
        en_BZ(0x2809, ServerCharset.CP1252),
        es_PE(0x280a, ServerCharset.CP1252),
        ar_JO(0x2c01, ServerCharset.CP1256),
        en_TT(0x2c09, ServerCharset.CP1252),
        es_AR(0x2c0a, ServerCharset.CP1252),
        ar_LB(0x3001, ServerCharset.CP1256),
        en_ZW(0x3009, ServerCharset.CP1252),
        es_EC(0x300a, ServerCharset.CP1252),
        ar_KW(0x3401, ServerCharset.CP1256),
        en_PH(0x3409, ServerCharset.CP1252),
        es_CL(0x340a, ServerCharset.CP1252),
        ar_AE(0x3801, ServerCharset.CP1256),
        es_UY(0x380a, ServerCharset.CP1252),
        ar_BH(0x3c01, ServerCharset.CP1256),
        es_PY(0x3c0a, ServerCharset.CP1252),
        ar_QA(0x4001, ServerCharset.CP1256),
        en_IN(0x4009, ServerCharset.CP1252),
        es_BO(0x400a, ServerCharset.CP1252),
        en_MY(0x4409, ServerCharset.CP1252),
        es_SV(0x440a, ServerCharset.CP1252),
        en_SG(0x4809, ServerCharset.CP1252),
        es_HN(0x480a, ServerCharset.CP1252),
        es_NI(0x4c0a, ServerCharset.CP1252),
        es_PR(0x500a, ServerCharset.CP1252),
        es_US(0x540a, ServerCharset.CP1252);

        private final int langID;

        private final ServerCharset serverCharset;

        WindowsLocale(int langID, ServerCharset serverCharset) {
            this.langID = langID;
            this.serverCharset = serverCharset;
        }

        ServerCharset getServerCharset() {
            this.serverCharset.charset();
            return this.serverCharset;
        }
    }

    /**
     * Enumeration of original SQL Server sort orders recognized by SQL Server.
     */
    enum SortOrder {
        BIN_CP437(30, "SQL_Latin1_General_CP437_BIN", ServerCharset.CP437),
        DICTIONARY_437(31, "SQL_Latin1_General_CP437_CS_AS", ServerCharset.CP437),
        NOCASE_437(32, "SQL_Latin1_General_CP437_CI_AS", ServerCharset.CP437),
        NOCASEPREF_437(33, "SQL_Latin1_General_Pref_CP437_CI_AS", ServerCharset.CP437),
        NOACCENTS_437(34, "SQL_Latin1_General_CP437_CI_AI", ServerCharset.CP437),
        BIN2_CP437(35, "SQL_Latin1_General_CP437_BIN2", ServerCharset.CP437),

        BIN_CP850(40, "SQL_Latin1_General_CP850_BIN", ServerCharset.CP850),
        DICTIONARY_850(41, "SQL_Latin1_General_CP850_CS_AS", ServerCharset.CP850),
        NOCASE_850(42, "SQL_Latin1_General_CP850_CI_AS", ServerCharset.CP850),
        NOCASEPREF_850(43, "SQL_Latin1_General_Pref_CP850_CI_AS", ServerCharset.CP850),
        NOACCENTS_850(44, "SQL_Latin1_General_CP850_CI_AI", ServerCharset.CP850),
        BIN2_CP850(45, "SQL_Latin1_General_CP850_BIN2", ServerCharset.CP850),

        CASELESS_34(49, "SQL_1xCompat_CP850_CI_AS", ServerCharset.CP850),
        BIN_ISO_1(50, "bin_iso_1", ServerCharset.CP1252),
        DICTIONARY_ISO(51, "SQL_Latin1_General_CP1_CS_AS", ServerCharset.CP1252),
        NOCASE_ISO(52, "SQL_Latin1_General_CP1_CI_AS", ServerCharset.CP1252),
        NOCASEPREF_ISO(53, "SQL_Latin1_General_Pref_CP1_CI_AS", ServerCharset.CP1252),
        NOACCENTS_ISO(54, "SQL_Latin1_General_CP1_CI_AI", ServerCharset.CP1252),
        ALT_DICTIONARY(55, "SQL_AltDiction_CP850_CS_AS", ServerCharset.CP850),
        ALT_NOCASEPREF(56, "SQL_AltDiction_Pref_CP850_CI_AS", ServerCharset.CP850),
        ALT_NOACCENTS(57, "SQL_AltDiction_CP850_CI_AI", ServerCharset.CP850),
        SCAND_NOCASEPREF(58, "SQL_Scandinavian_Pref_CP850_CI_AS", ServerCharset.CP850),
        SCAND_DICTIONARY(59, "SQL_Scandinavian_CP850_CS_AS", ServerCharset.CP850),
        SCAND_NOCASE(60, "SQL_Scandinavian_CP850_CI_AS", ServerCharset.CP850),
        ALT_NOCASE(61, "SQL_AltDiction_CP850_CI_AS", ServerCharset.CP850),

        DICTIONARY_1252(71, "dictionary_1252", ServerCharset.CP1252),
        NOCASE_1252(72, "nocase_1252", ServerCharset.CP1252),
        DNK_NOR_DICTIONARY(73, "dnk_nor_dictionary", ServerCharset.CP1252),
        FIN_SWE_DICTIONARY(74, "fin_swe_dictionary", ServerCharset.CP1252),
        ISL_DICTIONARY(75, "isl_dictionary", ServerCharset.CP1252),

        BIN_CP1250(80, "bin_cp1250", ServerCharset.CP1250),
        DICTIONARY_1250(81, "SQL_Latin1_General_CP1250_CS_AS", ServerCharset.CP1250),
        NOCASE_1250(82, "SQL_Latin1_General_CP1250_CI_AS", ServerCharset.CP1250),
        CSYDIC(83, "SQL_Czech_CP1250_CS_AS", ServerCharset.CP1250),
        CSYNC(84, "SQL_Czech_CP1250_CI_AS", ServerCharset.CP1250),
        HUNDIC(85, "SQL_Hungarian_CP1250_CS_AS", ServerCharset.CP1250),
        HUNNC(86, "SQL_Hungarian_CP1250_CI_AS", ServerCharset.CP1250),
        PLKDIC(87, "SQL_Polish_CP1250_CS_AS", ServerCharset.CP1250),
        PLKNC(88, "SQL_Polish_CP1250_CI_AS", ServerCharset.CP1250),
        ROMDIC(89, "SQL_Romanian_CP1250_CS_AS", ServerCharset.CP1250),
        ROMNC(90, "SQL_Romanian_CP1250_CI_AS", ServerCharset.CP1250),
        SHLDIC(91, "SQL_Croatian_CP1250_CS_AS", ServerCharset.CP1250),
        SHLNC(92, "SQL_Croatian_CP1250_CI_AS", ServerCharset.CP1250),
        SKYDIC(93, "SQL_Slovak_CP1250_CS_AS", ServerCharset.CP1250),
        SKYNC(94, "SQL_Slovak_CP1250_CI_AS", ServerCharset.CP1250),
        SLVDIC(95, "SQL_Slovenian_CP1250_CS_AS", ServerCharset.CP1250),
        SLVNC(96, "SQL_Slovenian_CP1250_CI_AS", ServerCharset.CP1250),
        POLISH_CS(97, "polish_cs", ServerCharset.CP1250),
        POLISH_CI(98, "polish_ci", ServerCharset.CP1250),

        BIN_CP1251(104, "bin_cp1251", ServerCharset.CP1251),
        DICTIONARY_1251(105, "SQL_Latin1_General_CP1251_CS_AS", ServerCharset.CP1251),
        NOCASE_1251(106, "SQL_Latin1_General_CP1251_CI_AS", ServerCharset.CP1251),
        UKRDIC(107, "SQL_Ukrainian_CP1251_CS_AS", ServerCharset.CP1251),
        UKRNC(108, "SQL_Ukrainian_CP1251_CI_AS", ServerCharset.CP1251),

        BIN_CP1253(112, "bin_cp1253", ServerCharset.CP1253),
        DICTIONARY_1253(113, "SQL_Latin1_General_CP1253_CS_AS", ServerCharset.CP1253),
        NOCASE_1253(114, "SQL_Latin1_General_CP1253_CI_AS", ServerCharset.CP1253),

        GREEK_MIXEDDICTIONARY(120, "SQL_MixDiction_CP1253_CS_AS", ServerCharset.CP1253),
        GREEK_ALTDICTIONARY(121, "SQL_AltDiction_CP1253_CS_AS", ServerCharset.CP1253),
        GREEK_ALTDICTIONARY2(122, "SQL_AltDiction2_CP1253_CS_AS", ServerCharset.CP1253),
        GREEK_NOCASEDICT(124, "SQL_Latin1_General_CP1253_CI_AI", ServerCharset.CP1253),
        BIN_CP1254(128, "bin_cp1254", ServerCharset.CP1254),
        DICTIONARY_1254(129, "SQL_Latin1_General_CP1254_CS_AS", ServerCharset.CP1254),
        NOCASE_1254(130, "SQL_Latin1_General_CP1254_CI_AS", ServerCharset.CP1254),

        BIN_CP1255(136, "bin_cp1255", ServerCharset.CP1255),
        DICTIONARY_1255(137, "SQL_Latin1_General_CP1255_CS_AS", ServerCharset.CP1255),
        NOCASE_1255(138, "SQL_Latin1_General_CP1255_CI_AS", ServerCharset.CP1255),

        BIN_CP1256(144, "bin_cp1256", ServerCharset.CP1256),
        DICTIONARY_1256(145, "SQL_Latin1_General_CP1256_CS_AS", ServerCharset.CP1256),
        NOCASE_1256(146, "SQL_Latin1_General_CP1256_CI_AS", ServerCharset.CP1256),

        BIN_CP1257(152, "bin_cp1257", ServerCharset.CP1257),
        DICTIONARY_1257(153, "SQL_Latin1_General_CP1257_CS_AS", ServerCharset.CP1257),
        NOCASE_1257(154, "SQL_Latin1_General_CP1257_CI_AS", ServerCharset.CP1257),
        ETIDIC(155, "SQL_Estonian_CP1257_CS_AS", ServerCharset.CP1257),
        ETINC(156, "SQL_Estonian_CP1257_CI_AS", ServerCharset.CP1257),
        LVIDIC(157, "SQL_Latvian_CP1257_CS_AS", ServerCharset.CP1257),
        LVINC(158, "SQL_Latvian_CP1257_CI_AS", ServerCharset.CP1257),
        LTHDIC(159, "SQL_Lithuanian_CP1257_CS_AS", ServerCharset.CP1257),
        LTHNC(160, "SQL_Lithuanian_CP1257_CI_AS", ServerCharset.CP1257),

        DANNO_NOCASEPREF(183, "SQL_Danish_Pref_CP1_CI_AS", ServerCharset.CP1252),
        SVFI1_NOCASEPREF(184, "SQL_SwedishPhone_Pref_CP1_CI_AS", ServerCharset.CP1252),
        SVFI2_NOCASEPREF(185, "SQL_SwedishStd_Pref_CP1_CI_AS", ServerCharset.CP1252),
        ISLAN_NOCASEPREF(186, "SQL_Icelandic_Pref_CP1_CI_AS", ServerCharset.CP1252),

        BIN_CP932(192, "bin_cp932", ServerCharset.CP932),
        NLS_CP932(193, "nls_cp932", ServerCharset.CP932),
        BIN_CP949(194, "bin_cp949", ServerCharset.CP949),
        NLS_CP949(195, "nls_cp949", ServerCharset.CP949),
        BIN_CP950(196, "bin_cp950", ServerCharset.CP950),
        NLS_CP950(197, "nls_cp950", ServerCharset.CP950),
        BIN_CP936(198, "bin_cp936", ServerCharset.CP936),
        NLS_CP936(199, "nls_cp936", ServerCharset.CP936),
        NLS_CP932_CS(200, "nls_cp932_cs", ServerCharset.CP932),
        NLS_CP949_CS(201, "nls_cp949_cs", ServerCharset.CP949),
        NLS_CP950_CS(202, "nls_cp950_cs", ServerCharset.CP950),
        NLS_CP936_CS(203, "nls_cp936_cs", ServerCharset.CP936),
        BIN_CP874(204, "bin_cp874", ServerCharset.CP874),
        NLS_CP874(205, "nls_cp874", ServerCharset.CP874),
        NLS_CP874_CS(206, "nls_cp874_cs", ServerCharset.CP874),

        EBCDIC_037(210, "SQL_EBCDIC037_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_273(211, "SQL_EBCDIC273_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_277(212, "SQL_EBCDIC277_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_278(213, "SQL_EBCDIC278_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_280(214, "SQL_EBCDIC280_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_284(215, "SQL_EBCDIC284_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_285(216, "SQL_EBCDIC285_CP1_CS_AS", ServerCharset.CP1252),
        EBCDIC_297(217, "SQL_EBCDIC297_CP1_CS_AS", ServerCharset.CP1252);

        private final int sortId;

        private final String name;

        private final ServerCharset serverCharset;

        SortOrder(int sortId, String name, ServerCharset serverCharset) {
            this.sortId = sortId;
            this.name = name;
            this.serverCharset = serverCharset;
        }

        ServerCharset getServerCharset() {
            this.serverCharset.charset();
            return this.serverCharset;
        }


        public final String toString() {
            return this.name;
        }
    }
}
