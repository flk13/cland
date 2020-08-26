package com.rynuk.cland.service.rpc;

import com.rynuk.cland.service.rpc.LocalWorkerCrawlerServiceBase.Iface;
import com.rynuk.cland.service.rpc.LocalWorkerCrawlerServiceBase.Processor;
import com.rynuk.cland.zk.worker.Worker;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public enum  ServiceServer {
    INSTANCE();

    private static final Logger logger = LoggerFactory.getLogger(ServiceServer.class);

    ServiceServer() {

    }

    Processor<WorkerCrawlerServiceImpl> processor;

    WorkerCrawlerServiceImpl impl;

    Optional<TServer> server = Optional.empty();

    static final int RPC_SERVER_PORT = 9001;

    public void init(Worker worker) {
        if (isServing()) {
            return;
        }
        try {
            impl = new WorkerCrawlerServiceImpl(worker);
            processor = new Processor<>(impl);
            TServerTransport serverTransport = new TServerSocket(RPC_SERVER_PORT);
            server = Optional.of(
                    new TThreadPoolServer(
                            new TThreadPoolServer.Args(serverTransport).processor(processor)));
        } catch (TTransportException e) {
            logger.error("rpc failed.", e);
        }
    }

    public boolean isServing() {
        return server.isPresent() && server.get().isServing();
    }

    public void stop() {
        logger.info("Stopping the local worker-crawler server...");
        server.ifPresent(TServer::stop);
    }

    public void run() {
        logger.info("Starting the local worker-crawler server...");
        server.ifPresent(TServer::serve);
    }

    /*SSL TODO*/
    public void secure(Processor<Iface> processor) {

    }
}
