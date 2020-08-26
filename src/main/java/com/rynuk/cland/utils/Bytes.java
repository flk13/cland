package com.rynuk.cland.utils;

/**
 * 字节类型数组转换，因为项目用到了私有协议通信
 * @author rynuk
 * @date 2020/7/22
 */

public class Bytes {
    //short与byte类型转换
    public static byte[] shortToBytes(short val) {
        byte[] res = new byte[2];
        res[0] = (byte) (val & 0xff);
        res[1] = (byte) (val >> 8 & 0xff);
        return res;
    }

    public static short bytesToShort(byte[] val) {
        return (short) (val[0] & 0xff | (val[1] & 0xff) << 8);
    }

    //long与byte类型转换
    public static byte[] longToBytes(long val) {
        byte[] res = new byte[8];
        res[0] = (byte) (val & 0xff);
        res[1] = (byte) (val >> 8 & 0xff);
        res[2] = (byte) (val >> 16 & 0xff);
        res[3] = (byte) (val >> 24 & 0xff);
        res[4] = (byte) (val >> 32 & 0xff);
        res[5] = (byte) (val >> 40 & 0xff);
        res[6] = (byte) (val >> 48 & 0xff);
        res[7] = (byte) (val >> 56 & 0xff);
        return res;
    }

    public static long bytesToLong(byte[] val) {
        return (val[0] & 0xff | (val[1] & 0xff) << 8 | (val[2] & 0xff) << 16 | (val[3] & 0xff) << 24
                | (long) (val[4] & 0xff) << 32 | (long) (val[5] & 0xff) << 40 | (long) (val[6] & 0xff) << 48 | (long) (val[7] & 0xff) << 56);
    }

    //int与byte类型转换
    public static byte[] intToBytes(int val) {
        byte[] res = new byte[4];
        res[0] = (byte) (val & 0xff);
        res[1] = (byte) (val >> 8 & 0xff);
        res[2] = (byte) (val >> 16 & 0xff);
        res[3] = (byte) (val >> 24 & 0xff);
        return res;
    }

    public static int bytesToInt(byte[] val) {
        return (val[0] & 0xff | (val[1] & 0xff) << 8 | (val[2] & 0xff) << 16 | (val[3] & 0xff) << 24);
    }
}
