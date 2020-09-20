package com.rynuk.cland.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * 使用NetworkInterface获得本机在局域网内的IP地址
 * 根据本机的IPv4地址来提供命名服务
 * 
 * @author rynuk
 * @date 2020/7/22
 */
public class IdProvider {
    private Logger logger = LoggerFactory.getLogger(IdProvider.class);

    public String getIp() {
        String id = "-1";
        try {
            // 获取本机所有网络接口
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                    continue;
                } else {
                    //获得与该网络接口绑定的IP地址，一般只有一个
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip != null && ip instanceof Inet4Address) {
                            id = ip.getHostAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Cannot get Id, something wrong with getting Ipv4 address");
        }
        return id;
    }
}
