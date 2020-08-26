package com.rynuk.cland.service.protocol.message;

/**
 * @author rynuk
 * @date 2020/7/27
 */
public enum  MessageType {

    LOGIN_REFUSED((byte)1), DUPLICATE_LOGIN((byte)2), HEART_BEAT_REQ((byte)3), HEART_BEAT_RESP((byte)4), CRAWLER_REQ((byte)5),
    CRAWLER_RESP((byte)6), SHELL_REQ((byte)7), SHELL_RESP((byte)8), LOGIN_ACCEPTED((byte)9);

    private byte rc;

    MessageType(byte rc){
        this.rc = rc;
    }

    public byte getValue(){
        return rc;
    }

    public static MessageType get(byte rc){
        switch (rc){
            case 1:
                //禁止登录
                return LOGIN_REFUSED;
            case 2:
                //已登录
                return DUPLICATE_LOGIN;
            case 3:
                //心跳检测请求
                return HEART_BEAT_REQ;
            case 4:
                //心跳检测收到
                return HEART_BEAT_RESP;
            case 5:
                //爬虫请求
                return CRAWLER_REQ;
            case 6:
                //爬虫收到
                return CRAWLER_RESP;
            case 7:
                //命令请求
                return SHELL_REQ;
            case 8:
                //命令收到
                return SHELL_RESP;
            case 9:
                //登录
                return LOGIN_ACCEPTED;
            default:
                //默认
                return null;
        }
    }
}
