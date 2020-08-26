package com.rynuk.cland.example.webmagic;

import com.rynuk.cland.service.local.CrawlerBootstrap;
import com.rynuk.cland.utils.InitLogger;
import io.netty.util.internal.ConcurrentSet;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 整个系统最开始是没有任务列表的，所以需要提前来导入一部分url数据
 * 这个类就是用来做预热的，所以和正式爬取类方法相同
 * @author rynuk
 * @date 2020/7/25
 */
public class WarmUp implements PageProcessor {
    private Site site = Site.me().setRetryTimes(3)
            .setSleepTime(1000).setUseGzip(true)
            .setUserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36");

    private static Set<String> newUrls = new ConcurrentSet<>();

    @Override
    public void process(Page page) {
        /* 只要获取的url数量大于100就终止爬虫任务 */
        if (newUrls.size() > 100) {
            try {
                CrawlerBootstrap.upLoadNewUrls(newUrls);
                System.exit(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }
        String html = page.getHtml().toString();
        String selfUrl = page.getUrl().toString();
        Pattern pattern = Pattern.compile("(?<=<a href=\")(?!" + selfUrl
                + ")https://en.wikipedia.org/wiki/.*?(?=\")");
        Matcher matcher = pattern.matcher(html);
        int counter = 0;
        while (matcher.find()) {
            if (counter < 5) {
                page.addTargetRequest(matcher.group());
                ++counter;
            } else {
                newUrls.add(matcher.group());
            }
        }
        System.out.println("current size: " + newUrls.size());
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        InitLogger.init();
        String startUrl = "https://en.wikipedia.org/wiki/Wiki";
        Spider.create(new WarmUp())
                .addUrl(startUrl)
                .thread(1)
                .run();
    }
}
