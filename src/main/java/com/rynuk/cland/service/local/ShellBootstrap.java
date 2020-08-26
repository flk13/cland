package com.rynuk.cland.service.local;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.service.protocol.Client;
import com.rynuk.cland.service.protocol.message.MessageType;
import com.rynuk.cland.service.protocol.message.ProcessDataProto.ProcessData;
import com.rynuk.cland.utils.InitLogger;


/**
 * @author rynuk
 * @date 2020/7/25
 */
public class ShellBootstrap extends Bootstrap {
    private static Configuration configuration = Configuration.INSTANCE;

    public ShellBootstrap(String command){
        ProcessData.Builder builder = ProcessData.newBuilder();
        builder.setType(MessageType.SHELL_REQ.getValue());
        builder.setCommand(command);
        super.client = new Client(builder.build());
    }

    @Override
    public void ready() {
        init();
    }

    public static void main(String[] args){
        InitLogger.initEmpty();
        StringBuilder command = new StringBuilder();
        for(String arg:args){
            command.append(arg);
            command.append(" ");
        }
        System.out.println("[info] command: " + command.toString());
        //Bootstrap bootstrap = new ShellBootstrap(command.toString());
        Bootstrap bootstrap = new ShellBootstrap("listtasks");
        bootstrap.ready();
    }
}
