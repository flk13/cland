package com.rynuk.cland.utils;

import org.fusesource.jansi.AnsiConsole;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * 用来设置cmd不同的输出颜色
 * @author rynuk
 * @date 2020/7/22
 */
public class Color {

    public static String error(String s) {
        AnsiConsole.systemInstall();
        String result = ansi().fgRed().a(s).reset().toString();
        AnsiConsole.systemUninstall();
        return result;
    }
}
