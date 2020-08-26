package com.rynuk.cland.filter;

import com.rynuk.cland.conf.StaticField;
import com.rynuk.cland.exception.FilterException;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bloom缓存文件的相关操作和信息
 * @author rynuk
 * @date 2020/7/25
 */
public class BloomFileInfo {
    /**
     * 命名格式： filePrefix#markup[1]#exp[2]#fpp[3]#urlCounter[4]#.fileSuffix
     */
    public static final String SEPARATOR = "#";

    private Long urlCounter;

    private Long expectedInsertions;

    private Double fpp;

    private int markup;

    public BloomFileInfo(String bloomFileName) throws FilterException.IllegalFilterCacheNameException {
        if(!(bloomFileName.startsWith(StaticField.BLOOM_CACHE_FILE_PREFIX)
                && bloomFileName.endsWith(StaticField.BLOOM_CACHE_FILE_SUFFIX))) {
            throw new FilterException.IllegalFilterCacheNameException(bloomFileName);
        }
        loadInfo(bloomFileName);
    }

    public BloomFileInfo(long urlCounter, long expectedInsertions, double fpp) {
        this.markup = StaticField.DEFAULT_FILTER_MARKUP;
        this.urlCounter = urlCounter;
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
    }

    public BloomFileInfo(int markup, long urlCounter, long expectedInsertions, double fpp) {
        this.markup = markup;
        this.urlCounter = urlCounter;
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
    }

    public void loadInfo(String bloomFileName) throws FilterException.IllegalFilterCacheNameException, NumberFormatException {
        String[] foo = bloomFileName.split(SEPARATOR);
        if(foo.length != 6) {
            throw new FilterException.IllegalFilterCacheNameException(bloomFileName);
        }
        markup = Integer.parseInt(foo[1]);
        expectedInsertions = Long.parseLong(foo[2]);
        fpp = Double.parseDouble(foo[3]);
        urlCounter = Long.parseLong(foo[4]);
    }

    /**
     * bloom过滤器的缓存文件的名字中包含着最大容量
     * 具体的值被 #_ 和 # 包裹起来
     *
     * @param bloomFileName bloom缓存文件的名字
     */
    @Deprecated
    private Long getUrlCounter(String bloomFileName) throws IOException {
        Long value = null;
        Pattern pattern = Pattern.compile("(?<=\\#_)\\d*(?=\\#)");
        Matcher matcher = pattern.matcher(bloomFileName);
        while (matcher.find()) {
            value = Long.parseLong(matcher.group());
            break;
        }
        if (value == null) {
            throw new IOException("Invaild bloom file name: "
                    + "get UrlCounter failed");
        }
        return value;
    }

    /**
     * bloom过滤器的缓存文件的名字中包含着已经录入元素的数量
     * 具体的值被 # 和 # 包裹起来
     *
     * @param bloomFileName bloom缓存文件的名字
     */
    @Deprecated
    private Long getExpectedInsertions(String bloomFileName) throws IOException {
        Long value = null;
        Pattern pattern = Pattern.compile("(?<=\\#)\\d*(?=\\#)");
        Matcher matcher = pattern.matcher(bloomFileName);
        while (matcher.find()) {
            value = Long.parseLong(matcher.group());
            break;
        }
        if (value == null) {
            throw new IOException("Invaild bloom file name: "
                    + "get ExpectedInsertions failed");
        }
        return value;
    }

    /**
     * bloom过滤器的缓存文件的名字中包含着误报概率
     * 具体的值被 # 和 _# 包裹起来
     *
     * @param bloomFileName bloom缓存文件的名字
     */
    @Deprecated
    private Double getFpp(String bloomFileName) throws IOException {
        Double value = null;
        Pattern pattern = Pattern.compile("(?<=\\#)0{1}\\.{1}\\d*(?=_\\#)");
        Matcher matcher = pattern.matcher(bloomFileName);
        while (matcher.find()) {
            value = Double.parseDouble(matcher.group());
            break;
        }
        if (value == null) {
            throw new IOException("Invaild bloom file name: "
                    + "get Fpp failed");
        }
        return value;
    }

    /**
     * bloom缓存文件名中包含了必要信息
     *
     * markup： 独立标识
     * urlCounter： 目前已经存入的URL的数量
     * expectedInsertions：最大容量
     * fpp： 误报概率
     *
     * @return
     */
    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("0.###############");
        return StaticField.BLOOM_CACHE_FILE_PREFIX
                + SEPARATOR + markup
                + SEPARATOR + urlCounter
                + SEPARATOR + expectedInsertions
                + SEPARATOR + df.format(fpp.doubleValue())
                + SEPARATOR + StaticField.BLOOM_CACHE_FILE_SUFFIX;
    }

    public Long getUrlCounter() {
        return urlCounter;
    }

    public void setUrlCounter(Long urlCounter) {
        this.urlCounter = urlCounter;
    }

    public Long getExpectedInsertions() {
        return expectedInsertions;
    }

    public Double getFpp() {
        return fpp;
    }

    public int getMarkup() {
        return markup;
    }
}
