package com.rynuk.cland.api.info;

import com.rynuk.cland.api.SimpleInfo;
import com.rynuk.cland.api.jsondata.FilterJson;
import com.rynuk.cland.api.jsondata.JData;
import com.rynuk.cland.exception.FilterException.IllegalFilterCacheNameException;
import com.rynuk.cland.filter.BloomFileInfo;
import com.rynuk.cland.saver.dfs.DFSManager;


import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author rynuk
 * @date 2020/7/28
 */
public class FilterInfo implements SimpleInfo {
    private DFSManager dfsManager;

    private List<JData> info;

    public FilterInfo(DFSManager dfsManager) {
        this.dfsManager = dfsManager;
        info = new LinkedList<>();
    }

    public FilterInfo getBloomCacheInfo(String src) throws IOException, IllegalFilterCacheNameException {
        List<String> filesPath = dfsManager.listFiles(src, false);
        for (String path : filesPath) {
            File file = new File(path);
            BloomFileInfo bloomFile = new BloomFileInfo(file.getName());
            FilterJson data = new FilterJson();
            try {
                data.setSize(dfsManager.getFileLen(path));
                data.setMtime(dfsManager.getFileModificationTime(path));
                data.setUniqueID(Integer.toString(bloomFile.getMarkup()));
                data.setFpp(bloomFile.getFpp());
                data.setMaxCapacity(bloomFile.getExpectedInsertions());
                data.setUrlsNum(bloomFile.getUrlCounter());
                info.add(data);
            } catch (Exception e) {
                // drop
            }
        }
        return this;
    }

    @Override
    public List<JData> getInfo() {
        return info;
    }
}
