package com.rynuk.cland.api.info;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.api.SimpleInfo;
import com.rynuk.cland.api.jsondata.JData;
import com.rynuk.cland.api.jsondata.WorkerJson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.LinkedList;
import java.util.List;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class WorkerInfo implements SimpleInfo {
    private List<JData> info;

    private CuratorFramework client;

    public WorkerInfo(CuratorFramework client) {
        this.info = new LinkedList<>();
        this.client = client;
    }

    public WorkerInfo getCurrentWoker() {
        List<String> children;
        try {
            children = client.getChildren().forPath(ZNodeStaticSetting.WORKERS_PATH);
            for (String child : children) {
                WorkerJson data = new WorkerJson();
                Stat stat = new Stat();
                byte[] content;
                content =
                        client.getData()
                                .storingStatIn(stat)
                                .forPath(ZNodeStaticSetting.WORKERS_PATH + '/' + child);
                data.setName(child);
                /* 32为md5码长度 */
                if (content.length == 32) {
                    data.setCurrentTask(new String(content));
                } else {
                    data.setCurrentTask("Empty");
                }
                info.add(data);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public List<JData> getInfo() {
        return info;
    }
}
