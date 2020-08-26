package com.rynuk.cland.service.protocol.handler;

import com.google.protobuf.ByteString;
import com.rynuk.cland.Configuration;
import com.rynuk.cland.service.local.Action;
import com.rynuk.cland.service.protocol.Client;
import com.rynuk.cland.service.protocol.message.MessageType;
import com.rynuk.cland.service.protocol.message.ProcessDataProto.ProcessData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author rynuk
 * @date 2020/7/27
 */
public class HeartBeatReqHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(LocalCrawlerHandler.class);
    private static Configuration configuration = Configuration.INSTANCE;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private volatile ScheduledFuture<?> heartBeat;
    private AtomicBoolean closeLongConnection;
    private Client client;
    private Action action;

    public HeartBeatReqHandler(Client client, AtomicBoolean closeLongConnection){
        this.client = client;
        this.action = client.getAction();
        this.closeLongConnection = closeLongConnection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        ProcessData message = (ProcessData)msg;
        MessageType type = MessageType.get((byte) message.getType());
        if (type == MessageType.HEART_BEAT_REQ) {
            heartBeat = ctx
                    .channel()
                    .eventLoop()
                    .scheduleAtFixedRate(new HeartBeatReqHandler.HeartBeatTask(ctx)
                            , configuration.WORKER_HEART_BEAT/3
                            , configuration.WORKER_HEART_BEAT/3, TimeUnit.SECONDS);
        } else if(type == MessageType.HEART_BEAT_RESP){
            String content = message.getAttachment().toString(Charset.defaultCharset());
            if(content != null){
                try {
                    action.reportResult(Integer.parseInt(content));
                } catch (NumberFormatException e){
                    //pass
                }
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        heartBeat.cancel(true);
        executor.execute(new KeepConnection(closeLongConnection));
    }


    private class KeepConnection implements Runnable {
        private AtomicBoolean closeLongConnection;

        public KeepConnection(AtomicBoolean closeLongConnection){
            this.closeLongConnection = closeLongConnection;
        }

        @Override
        public void run() {
            reconnect();
        }

        public void reconnect() {
            if(!closeLongConnection.get()) {
                logger.error("lose connection, trying to reconncet...");
                try {
                    TimeUnit.SECONDS.sleep(10);
                    client.connect();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e){
                    reconnect();
                }
            }
        }
    }

    private class HeartBeatTask implements Runnable {

        private final ChannelHandlerContext ctx;

        public HeartBeatTask(final ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            ProcessData message = buildHeatBeat();
            ctx.writeAndFlush(message);
        }

        private ProcessData buildHeatBeat() {
            Integer progress = action.report();
            ProcessData.Builder builder = ProcessData.newBuilder();
            builder.setType(MessageType.HEART_BEAT_REQ.getValue());
            builder.setAttachment(ByteString.copyFrom(progress.toString().getBytes()));
            return builder.build();
        }
    }
}
