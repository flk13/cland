package com.rynuk.cland.zk.resources;

import com.rynuk.cland.conf.Configuration;
import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.filter.URIBloomFilter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public enum  FilterINodesManagePool {
    INSTANCE;

    private Map<Integer, INode> nodesPool;

    private CuratorFramework client;

    private static final Logger logger = LoggerFactory.getLogger(FilterINodesManagePool.class);

    FilterINodesManagePool() {
        nodesPool = new ConcurrentHashMap<>();
    }

    public void init(CuratorFramework client) {
        this.client = client;
        logger.info("loading filters' node information...");
        loadExistNodes();
    }

    public void loadExistNodes() {
        Stat tmpStat = new Stat();
        try {
            List<String> groups = client.getChildren()
                    .forPath(ZNodeStaticSetting.FILTERS_ROOT);
            for (String group : groups) {
                List<String> nodes = client.getChildren()
                        .forPath(makeFilterZNodePath(group));
                for (String node : nodes) {
                    int markup = Integer.parseInt(node);
                    client.getData()
                            .storingStatIn(tmpStat)
                            .forPath(makeFilterZNodePath(group, markup));
                    FilterINode inode = new FilterINode(Instant.ofEpochMilli(tmpStat.getCtime())
                            , Instant.ofEpochMilli(tmpStat.getMtime()), group, markup);
                    nodesPool.put(markup, inode);
                }
            }
        } catch (KeeperException.ConnectionLossException e) {
            logger.warn("retrying : loadExistNodes");
            loadExistNodes();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public boolean createNewBloomFilter(long expectedInsertions, double fpp, String group, int uniqueMarkup) {
        boolean res = false;
        try {
            URIBloomFilter bloomFilter = new URIBloomFilter(expectedInsertions, fpp);
            bloomFilter.save((String) Configuration.INSTANCE.get(""));
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(makeFilterZNodePath(group, uniqueMarkup));
            FilterINode newNode = new FilterINode(Instant.now(), Instant.now(), group, uniqueMarkup);
            nodesPool.put(uniqueMarkup, newNode);
            res = true;
        } catch (Exception e) {
            logger.error("create a new filter failed. ", e.getMessage());
        }
        return res;
    }

    private String makeFilterZNodePath(String group, int uniqueMarkup) {
        return ZNodeStaticSetting.FILTERS_ROOT + ZNodeStaticSetting.PATH_SEPARATOR + group
                + ZNodeStaticSetting.PATH_SEPARATOR + Integer.toString(uniqueMarkup);
    }

    private String makeFilterZNodePath(String group) {
        return ZNodeStaticSetting.FILTERS_ROOT + ZNodeStaticSetting.PATH_SEPARATOR + group;
    }
}
