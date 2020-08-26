package com.rynuk.cland.service.local;

import com.rynuk.cland.service.protocol.Client;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public abstract class Bootstrap {
    protected Client client;

    abstract public void ready();

    public Bootstrap init(){
        client.connect();
        return this;
    }

    public Bootstrap close() {
        client.disconnect();
        return this;
    }
}
