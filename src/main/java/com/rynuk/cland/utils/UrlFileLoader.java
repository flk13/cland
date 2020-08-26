package com.rynuk.cland.utils;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 用一个list列表存储URL地址集合
 * @author rynuk
 * @date 2020/7/27
 */
public class UrlFileLoader {
    public List<String> readFileByLine(String filePath) throws IOException {
        File file = new File(filePath);
        return Files.readLines(file, Charset.defaultCharset());
    }
}
