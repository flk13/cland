package com.rynuk.cland;

import com.rynuk.cland.check.SelfTest;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.utils.IdProvider;
import com.rynuk.cland.utils.InitLogger;
import com.rynuk.cland.zk.manager.Manager;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 主程序启动入口
 *
 * @author rynuk
 * @date 2020/7/21 23:23
 */
@SuppressWarnings("restriction")
public enum  ClandMain {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(ClandMain.class);

    private String serverId;

    private Manager manager;

    private DFSManager dfsManager;

    private CuratorFramework client;

    private Configuration configuration;

    private ScheduledExecutorService manageExector = Executors.newScheduledThreadPool(1);

    ClandMain() {
        configuration = Configuration.INSTANCE;
        client = SelfTest.checkAndGetZK();
        serverId = new IdProvider().getIp();
        dfsManager = SelfTest.checkAndGetDFS();
        /* 监听kill信号 */
        SignalHandler handler = new StopSignalHandler();
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, handler);
    }

    /**
     * 定时执行manage
     */
    private void run() {
        manager = Manager.getInstance(client, serverId, dfsManager, configuration.getUrlFilter());
        manageExector.scheduleAtFixedRate(() -> {
            try {
                manager.manage();
            } catch (InterruptedException e) {
                logger.info("shut down.");
            } catch (Throwable e) {
                /*
                    TODO
                    还在考虑哪些需要 let it crash
                */
                logger.warn("something go wrong when managing: ", e);
            }
        }, 0, configuration.CHECK_TIME, TimeUnit.SECONDS);
    }

    private class StopSignalHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            try {
                logger.info("stopping manager...");
                manager.stop();
                manageExector.shutdownNow();
                client.close();
                dfsManager.close();
            } catch (Throwable e) {
                logger.error("handle|Signal handler" + "failed, reason "
                        + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (SelfTest.checkRunning(ClandMain.class.getSimpleName())) {
            logger.error("Service has been already running");
            System.exit(1);
        }
        InitLogger.init();
        ClandMain.INSTANCE.run();
    }
}
