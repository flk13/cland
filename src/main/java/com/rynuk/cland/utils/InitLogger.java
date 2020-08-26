package com.rynuk.cland.utils;

import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * log4j配置实例
 * 配置日志信息控制台打印
 * @author rynuk
 * @date 2020/7/25
 */
public class InitLogger {
    public static void init() {
        Properties prop = new Properties();

        prop.setProperty("log4j.rootLogger", "Info,CONSOLE");
        prop.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
        prop.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{HH:mm:ss,SSS} [%t] %-5p %C{1} : %m%n");

        PropertyConfigurator.configure(prop);
    }

    public static void initEmpty() {
        Properties prop = new Properties();
        prop.setProperty("log4j.rootLogger", "Error,CONSOLE");
        prop.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
        prop.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{HH:mm:ss,SSS} [%t] %-5p %C{1} : %m%n");

        PropertyConfigurator.configure(prop);
    }
}
