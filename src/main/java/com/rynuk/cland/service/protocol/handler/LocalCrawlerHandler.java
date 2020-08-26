package com.rynuk.cland.service.protocol.handler;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.saver.dfs.HDFSManager;
import com.rynuk.cland.service.local.Action;
import com.rynuk.cland.service.protocol.message.MessageType;
import com.rynuk.cland.service.protocol.message.ProcessDataProto.ProcessData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class LocalCrawlerHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(LocalCrawlerHandler.class);

    private static Configuration configuration = Configuration.INSTANCE;

    private static DFSManager hdfsManager = new HDFSManager(configuration.HDFS_SYSTEM_CONF
            , configuration.HDFS_SYSTEM_PATH);

    private static ExecutorService crawlerLoop = Executors.newFixedThreadPool(2);

    private Action action;

    public LocalCrawlerHandler(Action action) {
        this.action = action;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelInactive();
    }

    /**
     * 用户应该Override Action中的run方法
     * run方法实际上是传递了已经拿到的Url
     * 此时爬虫就可以开始任务了
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        ProcessData message = (ProcessData) msg;
        if (message.getType() == MessageType.CRAWLER_RESP.getValue()) {
            crawlerLoop.execute(new CrawlerTask(ctx, message));
        }
        ctx.fireChannelRead(msg);
    }

    private void process(ChannelHandlerContext ctx, ProcessData data) {
        String urlFilePath = data.getUrlFilePath();
        logger.info("Crawler get the task:" + urlFilePath
                + "success at {}", new Date().toString());
        try {
            hdfsManager.downloadFile(urlFilePath, configuration.TEMP_DIR);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*
            true 标识run成功，返回READY状态，领取下一个任务
            false 标识run失败，返回NULL状态，放弃该任务，让其他爬虫去领取该任务
         */
        String localSavePath = configuration.TEMP_DIR + '/'
                + data.getUrlFileName();
        //FIXME
        int progress = Integer.parseInt(data.getAttachment().toString(Charset.defaultCharset()));
        boolean flag = action.run(localSavePath, progress);
        /* 任务结束后删除url文件 */
        new File(localSavePath).delete();
        ProcessData.Builder builder = ProcessData.newBuilder();
        builder.setType(MessageType.CRAWLER_REQ.getValue());
        builder.setUrlFileName(data.getUrlFileName());
        builder.setStatus(flag ? ProcessData.CrawlerStatus.FINNISHED : ProcessData.CrawlerStatus.NULL);
        String result = flag ? "successed" : "failed";
        logger.info("Run task " + result);
        ctx.writeAndFlush(builder.build());
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
    }

    class CrawlerTask implements Runnable {
        ChannelHandlerContext ctx;
        ProcessData data;

        public CrawlerTask(ChannelHandlerContext ctx, ProcessData data) {
            this.ctx = ctx;
            this.data = data;
        }

        @Override
        public void run() {
            process(ctx, data);
        }
    }
}
