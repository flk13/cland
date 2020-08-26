package com.rynuk.cland.filter;

import com.rynuk.cland.exception.ClandRuntimeException;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;


/**
 * @author rynuk
 * @date 2020/8/11
 */
public class UrlFilterTest {
    @Test
    public void addAndExistTest() throws ClandRuntimeException.FilterOverflowException {
        String url1 = "baidu.com";
        String url2 = "zhihu.com";
        URIBloomFilter filter = new URIBloomFilter(1000000L, 1.0 / 1000000D);
        filter.put(url1);
        assertEquals(true, filter.mightContain(url1));
        assertEquals(false, filter.mightContain(url2));
    }

    @Test
    public void falseRateTest() throws ClandRuntimeException.FilterOverflowException, IOException {
        long testNum = 100000L;
        double fpp = 1.0D / testNum;
        int times = 2;
        long falseCounter = 0;
        URIBloomFilter filter = new URIBloomFilter(testNum, fpp);
        for (int i = 0; i < testNum; ++i) {
            filter.put("" + i);
        }
        for (long i = testNum; i < testNum * times; ++i) {
            boolean flag = filter.mightContain("" + i);
            if (flag) {
                falseCounter++;
            }
        }
        assert falseCounter / (double) testNum <= fpp * times;
    }
}
